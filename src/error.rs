use alloc::ffi::NulError;
use alloc::string::String;
use core::fmt;

use crate::types::RxErrorCode;

/// Crate-wide result type.
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// A `uhd_*` call returned non-zero. `code` is the raw `uhd_error`
    /// value; `message` is fetched from the matching `*_last_error`.
    #[cfg(feature = "uhd")]
    Uhd {
        code: u32,
        context: &'static str,
        message: String,
    },
    /// `recv` succeeded at the C level but the metadata carries a fatal
    /// stream error. The non-fatal codes (timeout, overflow) surface as
    /// flags on `RecvInfo` instead.
    Rx { code: RxErrorCode, message: String },
    /// A user-supplied string contained an interior NUL.
    InvalidString(NulError),
    /// A USB transfer or control request failed on a USB backend.
    Usb {
        backend: &'static str,
        context: &'static str,
        message: String,
    },
    /// Protocol-level failure on a USB backend: unexpected chip ID,
    /// PLL never locked, register readback mismatch.
    Device {
        backend: &'static str,
        context: &'static str,
        message: String,
    },
    /// The backend cannot do what was asked (DSP-manual tune on a device
    /// with no DSP stage, a rate outside hardware range, a feature the
    /// crate was built without).
    Unsupported { backend: &'static str, what: String },
    /// No device matched the open spec.
    NotFound { spec: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "uhd")]
            Error::Uhd {
                code,
                context,
                message,
            } => write!(f, "{context} failed (uhd_error {code}): {message}"),
            Error::Rx { code, message } => write!(f, "rx stream error {code:?}: {message}"),
            Error::InvalidString(e) => write!(f, "invalid string: {e}"),
            Error::Usb {
                backend,
                context,
                message,
            } => write!(f, "[{backend}] {context} failed: {message}"),
            Error::Device {
                backend,
                context,
                message,
            } => write!(f, "[{backend}] {context}: {message}"),
            Error::Unsupported { backend, what } => {
                write!(f, "[{backend}] unsupported: {what}")
            }
            Error::NotFound { spec } => write!(f, "no device matched \"{spec}\""),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Error::InvalidString(e) => Some(e),
            _ => None,
        }
    }
}

impl From<NulError> for Error {
    fn from(e: NulError) -> Self {
        Error::InvalidString(e)
    }
}

/// Run a blocking device-open sequence with a deadline.
///
/// The nusb enumerate/open/claim calls carry NO timeout of their own —
/// they are bare usbfs/IOKit syscalls — and a device whose firmware
/// wedged across a host reboot can still enumerate yet never complete
/// the next request. The opening thread then parks in the kernel at
/// zero CPU, silently, forever (observed on a Raspberry Pi 5 with a
/// HackRF Pro: an open sat exactly so for minutes, and the device later
/// fell off the bus entirely). An ABSENT device already fails fast via
/// [`Error::NotFound`] — this bounds the wedged-device case, which no
/// in-band error path can reach.
///
/// On deadline the worker thread is deliberately LEAKED: it is stuck
/// in an uninterruptible syscall, holds nothing the process needs
/// back, and dies with the process. One parked OS thread buys a loud,
/// prompt error instead of a silent hang.
#[cfg(feature = "std")]
pub fn open_with_deadline<T: Send + 'static>(
    backend: &'static str,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    /// A healthy open finishes in single-digit milliseconds; the
    /// slowest legitimate path is a cold hub enumeration (~1 s) plus
    /// the driver's own bounded first control transfer (sdr-rtl's 300 ms
    /// control timeout). 3 s is past every legitimate open and still
    /// "dies quickly" at a terminal.
    const OPEN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(3);
    open_with_deadline_at(backend, OPEN_DEADLINE, f)
}

/// Per backend, opens given up at their deadline whose thread has not yet
/// returned. While one exists, that thread may still be writing the
/// device's registers, so a new open of the backend is refused at once
/// rather than run interleaved with it.
#[cfg(feature = "std")]
static ABANDONED: std::sync::Mutex<alloc::vec::Vec<(&'static str, usize)>> =
    std::sync::Mutex::new(alloc::vec::Vec::new());

#[cfg(feature = "std")]
fn abandoned(backend: &'static str, delta: isize) -> usize {
    let mut v = ABANDONED.lock().unwrap_or_else(|p| p.into_inner());
    let i = match v.iter().position(|(b, _)| *b == backend) {
        Some(i) => i,
        None => {
            v.push((backend, 0));
            v.len() - 1
        }
    };
    v[i].1 = v[i].1.saturating_add_signed(delta);
    v[i].1
}

/// [`open_with_deadline`] with the deadline explicit — split out so the
/// timeout path is testable without a 3-second test.
#[cfg(feature = "std")]
fn open_with_deadline_at<T: Send + 'static>(
    backend: &'static str,
    deadline: std::time::Duration,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    use core::sync::atomic::{AtomicU8, Ordering};
    if abandoned(backend, 0) > 0 {
        return Err(Error::Usb {
            backend,
            context: "open",
            message: "an earlier open of this backend passed its deadline and is still running (it may still be writing the device); try again once it ends, or replug the device".into(),
        });
    }
    // 0 running, 1 returned in time, 2 given up (the thread then
    // un-counts itself when it returns).
    const RUNNING: u8 = 0;
    const RETURNED: u8 = 1;
    const GIVEN_UP: u8 = 2;
    let state = alloc::sync::Arc::new(AtomicU8::new(RUNNING));
    let thread_state = state.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(alloc::format!("{backend}-open"))
        .spawn(move || {
            let r = f();
            if thread_state
                .compare_exchange(RUNNING, RETURNED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                abandoned(backend, -1);
            }
            let _ = tx.send(r);
        })
        .expect("spawn device-open thread");
    match rx.recv_timeout(deadline) {
        Ok(result) => result,
        Err(_) => {
            if state
                .compare_exchange(RUNNING, GIVEN_UP, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                abandoned(backend, 1);
            } else if let Ok(result) = rx.try_recv() {
                // It returned just as the deadline passed.
                return result;
            }
            Err(Error::Usb {
                backend,
                context: "open",
                message: alloc::format!(
                    "no response in {deadline:?} — the device is not answering \
                     (wedged firmware? replug it or power-cycle its hub)"
                ),
            })
        }
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    #[test]
    fn bounded_open_returns_the_result_when_prompt() {
        let got =
            open_with_deadline_at("test-prompt", std::time::Duration::from_secs(1), || Ok(7u8));
        assert_eq!(got.unwrap(), 7);
    }

    #[test]
    fn bounded_open_errors_loudly_when_the_open_hangs() {
        // Stand-in for a wedged device: an open that never returns.
        let got: Result<u8> =
            open_with_deadline_at("test-hang", std::time::Duration::from_millis(50), || {
                std::thread::sleep(std::time::Duration::from_secs(60));
                Ok(0)
            });
        match got {
            Err(Error::Usb {
                backend: "test-hang",
                context: "open",
                message,
            }) => {
                assert!(message.contains("not answering"), "{message}");
            }
            other => panic!("expected the deadline error, got {other:?}"),
        }
    }

    /// Review A9: while an abandoned open still runs, a new open of the
    /// backend is refused at once (never two bring-ups at once); once it
    /// returns, opens work again.
    #[test]
    fn no_second_open_while_an_abandoned_one_runs() {
        let ms = std::time::Duration::from_millis;
        let slow: Result<u8> = open_with_deadline_at("test-a9", ms(50), move || {
            std::thread::sleep(ms(300));
            Ok(0)
        });
        assert!(slow.is_err());
        let t = std::time::Instant::now();
        let refused = open_with_deadline_at("test-a9", ms(1_000), || Ok(1u8));
        assert!(t.elapsed() < ms(50), "refused at once");
        assert!(alloc::format!("{}", refused.unwrap_err()).contains("still running"));
        std::thread::sleep(ms(400));
        assert_eq!(
            open_with_deadline_at("test-a9", ms(1_000), || Ok(2u8)).unwrap(),
            2
        );
    }
}
