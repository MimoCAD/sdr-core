//! Device-agnostic tuning and streaming types, shared by every backend.

/// Over-the-wire sample format between device and host. The host side always
/// receives sc16 ([`crate::Iq`]) regardless — smaller wire formats are
/// expanded on arrival — so this only trades USB bandwidth against wire
/// precision. Only the UHD backend has a choice to make; the nusb backends'
/// wire formats are fixed by hardware (8-bit) and ignore this.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OtwFormat {
    /// 16-bit I/Q, 4 bytes/sample. Bit-exact transport (224 MB/s at 56 Msps).
    #[default]
    Sc16,
    /// 12-bit I/Q, 3 bytes/sample — B2xx-specific, made for 56 Msps over
    /// USB 3. The AD936x ADC is 12-bit, so nothing real is lost; cuts USB
    /// load by 25%.
    Sc12,
    /// 8-bit I/Q, 2 bytes/sample. Halves USB load, drops 4 real bits.
    Sc8,
}

/// Fatal RX metadata error codes. The non-fatal codes (timeout, overflow)
/// surface as flags on [`RecvInfo`] instead of erroring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RxErrorCode {
    LateCommand,
    BrokenChain,
    Alignment,
    BadPacket,
    Unknown(u32),
}

/// Device time attached to a received buffer.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TimeSpec {
    pub full_secs: i64,
    pub frac_secs: f64,
}

/// USB-transport telemetry for one received chunk, measured at the bulk
/// transfer completions that produced it. `None` on UHD (libuhd hides its
/// transport); `Some` on the nusb backends. The RTL2832U has no
/// overflow flag — host-side loss is *inferred* from this timing, so keeping
/// the raw numbers visible is the point.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkStats {
    /// Bulk transfers folded into this chunk.
    pub transfers: u32,
    /// Worst gap between consecutive transfer completions, in microseconds.
    pub max_gap_us: u32,
    /// Completions that arrived later than the expected transfer duration
    /// by the backend's lateness threshold.
    pub late_transfers: u32,
    /// Samples inferred lost from timing gaps and short transfers.
    pub inferred_lost_samples: u64,
    /// The raw sample ledger: elapsed × rate − samples delivered, in
    /// samples. Its *slope* over time is the sum of sample-clock-vs-wall-
    /// clock drift and any sustained trickle loss (e.g. the RTL2832U's
    /// endpoint FIFO shedding sub-packet amounts at rates past its
    /// depth). The two are the same magnitude on ±25 ppm crystals, so no
    /// threshold is baked in here — on TCXO hardware a slope far above
    /// the crystal spec is a leak, and the counter test mode is the
    /// byte-exact arbiter either way.
    pub ledger_deficit_samples: i64,
    /// Monotonic time of the last completion, microseconds since stream start.
    pub completed_at_us: u64,
}

/// Everything the metadata reported about one `recv`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RecvInfo {
    pub n_samples: usize,
    /// Samples were lost upstream (the device-side `O`, or on the nusb
    /// backends an inferred host-side drop). Non-fatal.
    pub overflow: bool,
    /// The timeout elapsed before a packet arrived. Non-fatal; retry.
    pub timed_out: bool,
    pub out_of_sequence: bool,
    pub more_fragments: bool,
    pub time_spec: Option<TimeSpec>,
    /// USB-transport telemetry; `None` where the transport is opaque (UHD).
    pub link: Option<LinkStats>,
}

/// Frequency policy for one half (RF LO or DSP) of a tune request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TunePolicy {
    /// Leave that stage as-is (UHD policy NONE).
    Default,
    /// Let the driver pick (policy AUTO).
    Auto,
    /// Force this frequency in Hz (policy MANUAL).
    Manual(f64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TuneRequest {
    pub target_freq: f64,
    pub rf: TunePolicy,
    pub dsp: TunePolicy,
}

impl TuneRequest {
    /// The common case: tune everything automatically to `target_freq` Hz.
    pub fn auto(target_freq: f64) -> Self {
        TuneRequest {
            target_freq,
            rf: TunePolicy::Auto,
            dsp: TunePolicy::Auto,
        }
    }
}

/// Where the device actually ended up after a tune. All fields in Hz.
///
/// On backends with no DSP tuning stage (RTL, HackRF), `actual_dsp_freq` is
/// always 0 and `target_dsp_freq` carries the residual `target - actual_rf`
/// the application's own mixer must absorb; `actual_rf_freq` is re-derived
/// from the fractional-N registers actually written, not echoed.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TuneResult {
    pub clipped_rf_freq: f64,
    pub target_rf_freq: f64,
    pub actual_rf_freq: f64,
    pub target_dsp_freq: f64,
    pub actual_dsp_freq: f64,
}
