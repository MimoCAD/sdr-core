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
    /// the driver's own 500 ms-bounded first control transfer. 3 s is
    /// past every legitimate open and still "dies quickly" at a
    /// terminal.
    const OPEN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(3);
    open_with_deadline_at(backend, OPEN_DEADLINE, f)
}

/// [`open_with_deadline`] with the deadline explicit — split out so the
/// timeout path is testable without a 3-second test.
#[cfg(feature = "std")]
fn open_with_deadline_at<T: Send + 'static>(
    backend: &'static str,
    deadline: std::time::Duration,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(alloc::format!("{backend}-open"))
        .spawn(move || {
            let _ = tx.send(f());
        })
        .expect("spawn device-open thread");
    match rx.recv_timeout(deadline) {
        Ok(result) => result,
        Err(_) => Err(Error::Usb {
            backend,
            context: "open",
            message: alloc::format!(
                "no response in {deadline:?} — the device is not answering \
                 (wedged firmware? replug it or power-cycle its hub)"
            ),
        }),
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    #[test]
    fn bounded_open_returns_the_result_when_prompt() {
        let got = open_with_deadline_at("test", std::time::Duration::from_secs(1), || Ok(7u8));
        assert_eq!(got.unwrap(), 7);
    }

    #[test]
    fn bounded_open_errors_loudly_when_the_open_hangs() {
        // Stand-in for a wedged device: an open that never returns.
        let got: Result<u8> =
            open_with_deadline_at("test", std::time::Duration::from_millis(50), || {
                std::thread::sleep(std::time::Duration::from_secs(60));
                Ok(0)
            });
        match got {
            Err(Error::Usb { backend: "test", context: "open", message }) => {
                assert!(message.contains("not answering"), "{message}");
            }
            other => panic!("expected the deadline error, got {other:?}"),
        }
    }
}
