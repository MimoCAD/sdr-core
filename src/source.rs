//! The hot-path seam between a device backend and the async machinery.

use crate::Iq;
use crate::error::Result;
use crate::types::RecvInfo;

/// The minimal contract `iq_stream.rs` needs from a backend: everything else
/// about a device (tuning, gain, rates) happens before streaming starts.
/// Implemented by each backend's stream (`sdr-rtl`'s `RtlStream`,
/// `sdr-hackrf`'s `HackRfStream`) and by any front-end that wraps them.
pub trait RxSource: Send + 'static {
    /// Largest number of samples one `recv_into` call can deliver.
    fn max_num_samps(&self) -> usize;

    /// Begin continuous streaming. Stopping is Drop's job — the worker
    /// thread ends by dropping the source.
    fn start(&mut self) -> Result<()>;

    /// Blocking receive into `buf`. Timeout and overflow are non-fatal and
    /// come back as flags on the `RecvInfo`; only fatal stream errors `Err`.
    fn recv_into(&mut self, buf: &mut [Iq], timeout_secs: f64) -> Result<RecvInfo>;

    /// Samples the device did not produce, on purpose, since the last
    /// call: a change made inside a pause (an RTL-SDR's retune) stops the
    /// sampling for that long. A consumer keeping time by counting samples
    /// advances its clock by this, as it does by samples lost. 0 for a
    /// device that keeps sampling through a change.
    fn take_gap_samples(&mut self) -> u64 {
        0
    }

    /// Called once on the dedicated worker thread before the recv loop.
    /// Backends use it for best-effort realtime elevation — without it,
    /// desktop UI activity can preempt the recv loop long enough to overflow
    /// the USB transport (measured on macOS).
    fn setup_worker_thread(&self) {}
}

/// Best-effort realtime elevation for a backend's worker thread (libuhd
/// does its own via `uhd_set_thread_priority`). Hand-declared FFI, no
/// libc crate; failure is fine — streaming just overflows more under
/// desktop load.
#[cfg(all(feature = "std", target_os = "macos"))]
pub fn elevate_current_thread() {
    // QOS_CLASS_USER_INTERACTIVE = 0x21: the same scheduler tier the UI
    // boost that preempts us runs at.
    unsafe extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    unsafe {
        let _ = pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    }
}

#[cfg(all(feature = "std", target_os = "linux"))]
pub fn elevate_current_thread() {
    // SCHED_RR at a modest priority, matching what UHD requests.
    #[repr(C)]
    struct SchedParam {
        sched_priority: i32,
    }
    unsafe extern "C" {
        fn pthread_self() -> usize;
        fn pthread_setschedparam(thread: usize, policy: i32, param: *const SchedParam) -> i32;
        fn sched_get_priority_max(policy: i32) -> i32;
        fn sched_get_priority_min(policy: i32) -> i32;
    }
    const SCHED_RR: i32 = 2;
    let rc = unsafe {
        let (min, max) = (
            sched_get_priority_min(SCHED_RR),
            sched_get_priority_max(SCHED_RR),
        );
        let param = SchedParam {
            sched_priority: min + (max - min) * 3 / 4,
        };
        pthread_setschedparam(pthread_self(), SCHED_RR, &param)
    };
    // Refusal MUST be loud. An unprivileged process (RLIMIT_RTPRIO 0,
    // no CAP_SYS_NICE) is refused with EPERM — the recv loop then runs
    // at normal priority and loses scheduler races to busy processing
    // threads exactly at peak load. A 12-hour run on a Raspberry Pi 5
    // shed ~46 seconds of samples that way, in proportion to load, and
    // with the refusal silent nothing said why.
    if rc != 0 {
        std::eprintln!(
            "source: realtime elevation REFUSED (pthread_setschedparam errno {rc}) — the USB \
             recv loop runs at normal priority and can lose samples under peak load; \
             grant it: sudo setcap cap_sys_nice+ep <path-to-binary>  (re-apply after every \
             rebuild)"
        );
    }
}

#[cfg(all(feature = "std", not(any(target_os = "macos", target_os = "linux"))))]
pub fn elevate_current_thread() {}
