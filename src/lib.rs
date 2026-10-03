//! The shared layer of the `sdr-*` radio drivers: one complex sample
//! ([`Iq`]), one error type, the device-agnostic tuning and gain types,
//! and the streaming machinery — [`RxSource`], the seam a backend
//! implements, and [`IqStream`], the runtime-agnostic
//! `futures_core::Stream` that moves a blocking `recv` loop onto its own
//! thread. With the `usb` feature, the bulk-IQ engine the USB drivers
//! stream through lives here too (`usb_stream`).
//!
//! `no_std` at its core (with `alloc`): the sample, the error, the tuning
//! and gain types, [`RxSource`] and the sample clock need nothing else.
//! The default `std` feature adds what needs threads and the OS:
//! [`IqStream`], [`open_with_deadline`], [`elevate_current_thread`]; `usb`
//! (which implies `std`) the bulk-IQ engine.

#![no_std]

extern crate alloc;
#[cfg(feature = "std")]
#[macro_use]
extern crate std;

pub mod clock;
mod device;
mod error;
pub mod gain;
#[cfg(feature = "std")]
mod iq_stream;
#[cfg(feature = "std")]
mod ring;
mod source;
#[cfg(feature = "std")]
pub mod sync;
#[cfg(feature = "std")]
pub mod time;
mod types;
#[cfg(feature = "usb")]
pub mod usb_stream;

#[cfg(all(feature = "std", not(target_arch = "wasm32")))]
pub use sync::block_on;

pub use device::{Backend, DeviceDesc};
#[cfg(feature = "std")]
pub use error::open_with_deadline;
pub use error::{Error, Result};
#[cfg(feature = "std")]
pub use iq_stream::{IqChunk, IqStream};
pub use source::RxSource;
#[cfg(feature = "std")]
pub use source::elevate_current_thread;
pub use types::{
    LinkStats, OtwFormat, RecvInfo, RxErrorCode, TimeSpec, TunePolicy, TuneRequest, TuneResult,
};

/// One complex sample in `sc16` CPU format: I then Q, interleaved. Backends
/// with narrower ADC paths widen into this on arrival (left-justified), so
/// full scale is ±32768 regardless of hardware.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Iq {
    pub i: i16,
    pub q: i16,
}

// recv writes raw sc16 pairs straight into `[Iq]` buffers; that is only
// sound if the layouts agree exactly.
const _: () = {
    assert!(core::mem::size_of::<Iq>() == 2 * core::mem::size_of::<i16>());
    assert!(core::mem::align_of::<Iq>() == core::mem::align_of::<i16>());
};
