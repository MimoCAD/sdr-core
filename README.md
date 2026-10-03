# sdr-core

The shared layer of the `sdr-*` radio drivers ([`sdr-rtl`], [`sdr-hackrf`]):

- `Iq` — one complex sample, sc16 (I then Q), full scale ±32768 whatever
  the hardware's ADC width.
- `Error` / `Result` — one error type for every backend, and
  `open_with_deadline`, which bounds a device open that a wedged device
  would otherwise hang forever.
- `TuneRequest` / `TuneResult` / `TunePolicy`, `RecvInfo`, `LinkStats` —
  device-agnostic tuning and streaming metadata.
- `gain::Stage` / `StageReading` — receive gain stage by stage (amp, LNA,
  mixer, VGA, PGA), each set by hardware step: the shared names, which
  each driver's own `Stage` (only the stages its radio has) converts to
  and from.
- `RxSource` — the hot-path contract a backend's stream implements — and
  `IqStream`, a runtime-agnostic `futures_core::Stream` of IQ chunks that
  runs the blocking receive loop on its own thread with a drop-oldest
  queue.
- With the `usb` feature: `usb_stream::BulkIqStream`, a bulk-transfer IQ
  engine over [nusb] that infers the host-side sample loss the hardware
  never reports, from transfer timing and a sample ledger; it hands out
  samples widened to `Iq` or the wire bytes as sent, and holds a stream
  at an exact sample position for a deliberate break.

`no_std` at its core (with `alloc`): `Iq`, `Error`, the tuning and gain
types, `RxSource` and the sample clock. The default `std` feature adds
`IqStream`, `open_with_deadline` and thread elevation (with
`futures-core`); `usb` implies `std`.

[`sdr-rtl`]: https://crates.io/crates/sdr-rtl
[`sdr-hackrf`]: https://crates.io/crates/sdr-hackrf
[nusb]: https://crates.io/crates/nusb

License: MIT OR Apache-2.0.
