//! What enumeration hands back: which driver a device belongs to and the
//! spec string that opens it. Shared so each driver's `list()` builds its
//! entries in one shape, whatever front-end collects them.

use alloc::format;
use alloc::string::String;

/// Which driver a device or descriptor belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Usrp,
    Rtl,
    HackRf,
}

/// One enumerated device, as a driver's `list()` reports it.
#[derive(Clone, Debug)]
pub struct DeviceDesc {
    /// The spec string that opens this device: `<scheme>:<args>`, e.g.
    /// `rtl:serial=00000001`, `hackrf:serial=…`, `uhd:type=b200,…`.
    pub spec: String,
    /// Human-readable one-liner (product, serial).
    pub label: String,
    pub backend: Backend,
}

impl DeviceDesc {
    /// The spec that opens this device at the given
    /// sample rate. For a USRP a nonzero rate pins the master clock at
    /// 2× (the anti-spur rule: an unpinned B2xx clock can land ON the
    /// rate — FPGA decimation 1 — where the AD9364 plants ±270–310 kHz
    /// spur sidebands at −30 dBc around every strong carrier); the USB
    /// backends have no master clock and pass through.
    pub fn open_spec(&self, rate_hz: f64) -> String {
        match self.backend {
            Backend::Usrp if rate_hz > 0.0 => {
                let sep = if self.spec.ends_with(':') { "" } else { "," };
                format!("{}{sep}master_clock_rate={}", self.spec, 2.0 * rate_hz)
            }
            _ => self.spec.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The anti-spur pin rides only on a USRP's spec, appended to
    /// whatever `uhd_usrp_find` reported; rate 0 means "leave the
    /// clock to UHD", and the USB backends pass through untouched.
    #[test]
    fn open_spec_pins_the_usrp_clock_and_leaves_usb_backends_alone() {
        let usrp = DeviceDesc {
            spec: "uhd:type=b200,serial=0000001".into(),
            label: String::new(),
            backend: Backend::Usrp,
        };
        assert_eq!(
            usrp.open_spec(4.8e6),
            "uhd:type=b200,serial=0000001,master_clock_rate=9600000"
        );
        assert_eq!(usrp.open_spec(0.0), "uhd:type=b200,serial=0000001");
        // A bare scheme grows no stray comma.
        let bare = DeviceDesc {
            spec: "uhd:".into(),
            label: String::new(),
            backend: Backend::Usrp,
        };
        assert_eq!(bare.open_spec(2.4e6), "uhd:master_clock_rate=4800000");
        let rtl = DeviceDesc {
            spec: "rtl:0".into(),
            label: String::new(),
            backend: Backend::Rtl,
        };
        assert_eq!(rtl.open_spec(2.4e6), "rtl:0", "no master clock to pin");
    }
}
