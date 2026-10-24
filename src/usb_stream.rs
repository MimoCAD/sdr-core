//! Shared bulk-IQ streaming engine for the nusb backends.
//!
//! Neither the RTL2832U nor the HackRF reports host-side loss: when the
//! host falls behind, bytes vanish silently and the stream continues,
//! misaligned in time. The only place that loss is visible is transfer
//! timing — so a submission queue of bulk transfers is kept full, every
//! completion is timestamped, and gaps beyond what the sample clock can
//! explain are counted as inferred loss, surfaced both in [`LinkStats`]
//! and as the `overflow` flag existing consumers already annotate.
//!
//! One async implementation for every target: in a browser (WebUSB,
//! `wasm32`) every wait is a real await; natively nusb's waits block, so
//! the async methods complete without suspending, and the blocking
//! methods (`recv_into`, `hold`, …, native only) run them through
//! [`crate::block_on`]. WebUSB cannot cancel a queued transfer, so there a
//! hold waits for the completions to stop instead of cancelling.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::boxed::Box;
use std::string::{String, ToString};
use std::vec::Vec;

use nusb::transfer::{Buffer, Bulk, Completion, In, TransferError};

use crate::Iq;
use crate::error::{Error, Result};
use crate::time::Instant;
use crate::types::{LinkStats, RecvInfo};

/// In a browser a hold cannot cancel the queue: it takes completions until
/// none comes for this long after the device's output stopped. Unmeasured
/// on WebUSB; the native hold cancels instead.
#[cfg(target_arch = "wasm32")]
pub const HOLD_QUIET: Duration = Duration::from_millis(20);
/// How long a native hold or stop waits on each cancelled transfer.
#[cfg(not(target_arch = "wasm32"))]
const CANCEL_WAIT: Duration = Duration::from_millis(200);

/// The bulk IN endpoint a [`BulkIqStream`] runs on: nusb's endpoint on a
/// real device, or a stand-in that scripts the device's side (the
/// [`mock`] endpoint, for tests that cannot be run on hardware: stalls,
/// faults, cancels, short and odd-length transfers). The calls are nusb's
/// own, so the engine is the same code against either.
pub trait BulkIn {
    /// The endpoint's maximum packet size, bytes.
    fn max_packet_size(&self) -> usize;
    /// A buffer for a transfer of `len` bytes.
    fn allocate(&self, len: usize) -> Buffer;
    /// Queue a transfer.
    fn submit(&mut self, buf: Buffer);
    /// Transfers submitted and not yet taken back as completions.
    fn pending(&self) -> usize;
    /// Cancel every queued transfer: each still completes, with
    /// `TransferError::Cancelled` and whatever it had received. Native
    /// only: WebUSB cannot cancel a queued transfer.
    #[cfg(not(target_arch = "wasm32"))]
    fn cancel_all(&mut self);
    /// The next completion, without waiting.
    fn poll_next_complete(&mut self, cx: &mut Context<'_>) -> Poll<Completion>;
    /// The next completion within `timeout`, `None` if none came.
    #[cfg(not(target_arch = "wasm32"))]
    fn wait_next_complete(&mut self, timeout: Duration) -> Option<Completion>;
    /// Clear a halt (a stall). nusb: not while transfers are pending.
    fn clear_halt(&mut self) -> Pin<Box<dyn Future<Output = core::result::Result<(), String>> + '_>>;
}

impl BulkIn for nusb::Endpoint<Bulk, In> {
    fn max_packet_size(&self) -> usize {
        nusb::Endpoint::max_packet_size(self)
    }

    fn allocate(&self, len: usize) -> Buffer {
        nusb::Endpoint::allocate(self, len)
    }

    fn submit(&mut self, buf: Buffer) {
        nusb::Endpoint::submit(self, buf)
    }

    fn pending(&self) -> usize {
        nusb::Endpoint::pending(self)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn cancel_all(&mut self) {
        nusb::Endpoint::cancel_all(self)
    }

    fn poll_next_complete(&mut self, cx: &mut Context<'_>) -> Poll<Completion> {
        nusb::Endpoint::poll_next_complete(self, cx)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn wait_next_complete(&mut self, timeout: Duration) -> Option<Completion> {
        nusb::Endpoint::wait_next_complete(self, timeout)
    }

    fn clear_halt(&mut self) -> Pin<Box<dyn Future<Output = core::result::Result<(), String>> + '_>> {
        #[cfg(not(target_arch = "wasm32"))]
        let r = nusb::MaybeFuture::wait(nusb::Endpoint::clear_halt(self)).map_err(|e| e.to_string());
        #[cfg(not(target_arch = "wasm32"))]
        return Box::pin(core::future::ready(r));
        #[cfg(target_arch = "wasm32")]
        return Box::pin(async move { nusb::Endpoint::clear_halt(self).await.map_err(|e| e.to_string()) });
    }
}

#[cfg(all(any(test, feature = "mock"), not(target_arch = "wasm32")))]
pub mod mock;

/// A boxed [`BulkIn`]: `Send` natively, where the stream crosses threads.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedBulkIn = Box<dyn BulkIn + Send>;
/// A boxed [`BulkIn`].
#[cfg(target_arch = "wasm32")]
pub type BoxedBulkIn = Box<dyn BulkIn>;

/// How the device packs samples on the wire.
#[derive(Clone, Copy, Debug)]
pub enum WireSample {
    /// Unsigned 8-bit, offset-binary around 128 (RTL2832U).
    OffsetU8,
    /// Signed 8-bit two's complement (HackRF).
    I8,
}

impl WireSample {
    /// One wire sample widened to sc16 (full scale ±32768), as
    /// [`BulkIqStream::recv_into`] widens.
    #[inline]
    pub fn widen(self, i: u8, q: u8) -> Iq {
        match self {
            WireSample::OffsetU8 => Iq {
                i: ((i as i16) - 128) << 8,
                q: ((q as i16) - 128) << 8,
            },
            WireSample::I8 => Iq {
                i: ((i as i8) as i16) << 8,
                q: ((q as i8) as i16) << 8,
            },
        }
    }
}

/// Linux's ENOMEM, which usbfs returns when a submission would pass the
/// host's `usbfs_memory_mb` limit (nusb passes it on as
/// `TransferError::Unknown`).
#[cfg(target_os = "linux")]
const LINUX_ENOMEM: u32 = 12;

/// A failed transfer's message: the host's usbfs memory limit named when
/// that is what refused it (sdr-rtl findings 21: 1 and 2 MB refuse a
/// 128 × 16 KiB queue, 3 MB does not).
fn transfer_message(e: TransferError, transfer_bytes: usize, queue_depth: usize) -> String {
    #[cfg(target_os = "linux")]
    if e == TransferError::Unknown(LINUX_ENOMEM) {
        return std::format!(
            "out of usbfs memory (ENOMEM): this stream queues {queue_depth} transfers of {} KiB ({:.1} MiB); raise /sys/module/usbcore/parameters/usbfs_memory_mb (0 = no limit)",
            transfer_bytes / 1024,
            (queue_depth * transfer_bytes) as f64 / (1024.0 * 1024.0)
        );
    }
    let _ = (transfer_bytes, queue_depth);
    e.to_string()
}

/// A running bulk RX queue. One `recv_into` call consumes one completion;
/// 8-bit wire samples widen into the caller's sc16 buffer during the copy.
pub struct BulkIqStream {
    backend: &'static str,
    endpoint: BoxedBulkIn,
    fmt: WireSample,
    transfer_bytes: usize,
    queue_depth: usize,
    sample_rate: f64,
    started: Option<Instant>,
    /// Completion time of the previous transfer, µs since `started`.
    last_complete_us: u64,
    /// Transfers completed since `start()`.
    completed: u64,
    /// Wire bytes handed to the caller since `start()`, as samples
    /// ([`BulkIqStream::recv_into`], two bytes each) or as bytes
    /// ([`BulkIqStream::recv_bytes`]).
    delivered_bytes: u64,
    /// Ledger baseline: the deficit's resting level. Creeps slowly toward
    /// the observed deficit while the stream is healthy, absorbing sample-
    /// clock-vs-wall-clock drift (~10⁻⁴ or less) without ever absorbing a
    /// step change.
    deficit_floor: f64,
    /// Consecutive dequeues with the ledger margin above threshold. A
    /// stall the queue absorbed pays its deficit back within one queue
    /// cycle of dequeues; real loss stays raised past it.
    over_since: usize,
    /// Carries an unpaired trailing byte across transfers (odd-length
    /// completions would otherwise swap I and Q for the rest of the run).
    pending_byte: Option<u8>,
    /// Lateness beyond expected×this counts as a gap.
    gap_factor: f64,
    /// Bytes taken off the wire and not yet handed to the caller, in wire
    /// order, served before any new completion: what [`BulkIqStream::hold`]
    /// took (completed or cut short by the cancel), or the part of a
    /// completion that did not fit a [`BulkIqStream::recv_bytes`] buffer.
    backlog: Vec<u8>,
    /// Bytes of `backlog` already handed out.
    backlog_read: usize,
    /// When the hold began; `None` while running.
    held_at: Option<Instant>,
    /// Set by [`BulkIqStream::release`]: the next completion sets the
    /// ledger's floor afresh (the device was quiet by design).
    rebaseline: bool,
    /// Every sample the ledger inferred lost since `new`, whichever call
    /// took the completion ([`BulkIqStream::inferred_lost_total`]).
    lost_total: u64,
}

impl BulkIqStream {
    pub fn new(
        backend: &'static str,
        endpoint: nusb::Endpoint<Bulk, In>,
        fmt: WireSample,
        transfer_bytes: usize,
        queue_depth: usize,
        sample_rate: f64,
    ) -> BulkIqStream {
        BulkIqStream::with_endpoint(backend, Box::new(endpoint), fmt, transfer_bytes, queue_depth, sample_rate)
    }

    /// [`BulkIqStream::new`] on any [`BulkIn`].
    pub fn with_endpoint(backend: &'static str, endpoint: BoxedBulkIn, fmt: WireSample, transfer_bytes: usize, queue_depth: usize, sample_rate: f64) -> BulkIqStream {
        // Transfers must be multiples of the endpoint's max packet size or
        // the bus rounds awkwardly; snap down, floor one packet.
        let mps = endpoint.max_packet_size().max(64);
        let transfer_bytes = (transfer_bytes.max(mps) / mps) * mps;
        BulkIqStream {
            backend,
            endpoint,
            fmt,
            transfer_bytes,
            queue_depth: queue_depth.max(2),
            sample_rate,
            started: None,
            last_complete_us: 0,
            completed: 0,
            delivered_bytes: 0,
            deficit_floor: 0.0,
            over_since: 0,
            pending_byte: None,
            gap_factor: 1.5,
            backlog: Vec::new(),
            backlog_read: 0,
            held_at: None,
            rebaseline: false,
            lost_total: 0,
        }
    }

    /// Every sample the ledger inferred lost since the stream was made:
    /// what each receive reports in `LinkStats::inferred_lost_samples`,
    /// plus what the completions taken by a hold, a reap or an absorb
    /// carried.
    pub fn inferred_lost_total(&self) -> u64 {
        self.lost_total
    }

    /// Samples handed to the caller since `start()` (wire bytes / 2).
    pub fn delivered(&self) -> u64 {
        self.delivered_bytes / 2
    }

    /// Wire bytes handed to the caller since `start()`.
    pub fn delivered_bytes(&self) -> u64 {
        self.delivered_bytes
    }

    /// Whether the stream is held ([`BulkIqStream::hold`]).
    pub fn is_held(&self) -> bool {
        self.held_at.is_some()
    }

    /// Bytes taken off the wire and not yet handed out: the backlog's
    /// unread part and the carry byte.
    fn on_hand_bytes(&self) -> u64 {
        (self.backlog.len() - self.backlog_read + self.pending_byte.is_some() as usize) as u64
    }

    /// The break position `hold` reports: samples handed out plus on hand.
    fn position(&self) -> u64 {
        (self.delivered_bytes + self.on_hand_bytes()) / 2
    }

    /// Take everything the device has put on the wire, for a break the
    /// device makes on purpose (a backend pauses its output first): every
    /// queued transfer is cancelled and what each holds — whole or cut
    /// short — kept, in order, to be handed out by `recv_into` before
    /// anything after the break. Nothing is resubmitted until
    /// [`BulkIqStream::release`].
    ///
    /// Returns the break's position: the samples the caller will have
    /// received when the backlog is drained — `delivered()` now plus the
    /// backlog. Exact as long as the device's output had stopped before
    /// the call (whatever it still sends lands after the break).
    ///
    /// In a browser the queued transfers cannot be cancelled: the hold
    /// takes completions until none comes for [`HOLD_QUIET`], and the
    /// transfers still queued stay queued, to be filled after the resume.
    pub async fn hold_async(&mut self) -> Result<u64> {
        if self.started.is_none() || self.held_at.is_some() {
            return Ok(self.position());
        }
        #[cfg(not(target_arch = "wasm32"))]
        let wait = {
            self.endpoint.cancel_all();
            CANCEL_WAIT
        };
        #[cfg(target_arch = "wasm32")]
        let wait = HOLD_QUIET;
        let mut stalled = false;
        while self.endpoint.pending() > 0 {
            let Some(c) = self.next_completion(wait).await else {
                break;
            };
            self.backlog.extend_from_slice(&c.buffer[..c.actual_len]);
            match c.status {
                Ok(()) | Err(TransferError::Cancelled) => {}
                // Cleared once nothing is pending (nusb: not with transfers
                // queued).
                Err(TransferError::Stall) => stalled = true,
                Err(e) => {
                    return Err(Error::Usb {
                        backend: self.backend,
                        context: "bulk hold",
                        message: e.to_string(),
                    });
                }
            }
        }
        self.held_at = Some(Instant::now());
        if stalled {
            if self.endpoint.pending() > 0 {
                return Err(Error::Usb {
                    backend: self.backend,
                    context: "bulk hold",
                    message: "the endpoint stalled and transfers were still pending after the cancel; the halt cannot be cleared".into(),
                });
            }
            self.clear_halt("bulk hold").await?;
        }
        Ok(self.position())
    }

    /// [`BulkIqStream::hold_async`], blocking.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn hold(&mut self) -> Result<u64> {
        crate::block_on(self.hold_async())
    }

    /// The next completion within `timeout`, `None` if none came.
    #[cfg(not(target_arch = "wasm32"))]
    async fn next_completion(&mut self, timeout: Duration) -> Option<Completion> {
        self.endpoint.wait_next_complete(timeout)
    }

    /// The next completion within `timeout`, `None` if none came.
    #[cfg(target_arch = "wasm32")]
    async fn next_completion(&mut self, timeout: Duration) -> Option<Completion> {
        use core::future::{Future, poll_fn};
        use core::task::Poll;
        let mut sleep = core::pin::pin!(crate::time::sleep(timeout));
        let endpoint = &mut self.endpoint;
        poll_fn(|cx| {
            if let Poll::Ready(c) = endpoint.poll_next_complete(cx) {
                return Poll::Ready(Some(c));
            }
            sleep.as_mut().poll(cx).map(|()| None)
        })
        .await
    }

    /// Clear a stalled endpoint; a failure is the stream's error (an
    /// endpoint left halted would only ever time out).
    async fn clear_halt(&mut self, context: &'static str) -> Result<()> {
        let backend = self.backend;
        self.endpoint.clear_halt().await.map_err(|message| Error::Usb { backend, context, message })
    }

    /// Recover from a stall in the running queue: every queued transfer
    /// cancelled and drained (what they carry kept, in order), the halt
    /// cleared with nothing pending, the queue submitted afresh. The stall
    /// itself shows up in the ledger as a gap. In a browser the queue cannot
    /// be cancelled, so the halt is cleared with it queued.
    async fn recover_stall(&mut self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.endpoint.cancel_all();
            while self.endpoint.pending() > 0 {
                let Some(c) = self.endpoint.wait_next_complete(CANCEL_WAIT) else {
                    return Err(Error::Usb {
                        backend: self.backend,
                        context: "bulk stall",
                        message: "cancelled transfers did not complete; the halt cannot be cleared".into(),
                    });
                };
                self.backlog.extend_from_slice(&c.buffer[..c.actual_len]);
            }
        }
        self.clear_halt("bulk stall").await?;
        self.fill_queue();
        Ok(())
    }

    /// Submit transfers until `queue_depth` are pending.
    fn fill_queue(&mut self) {
        while self.endpoint.pending() < self.queue_depth {
            let mut buf = self.endpoint.allocate(self.transfer_bytes);
            buf.set_requested_len(self.transfer_bytes);
            self.endpoint.submit(buf);
        }
    }

    /// End a hold: the queue resubmitted, the held interval taken out of
    /// the ledger's clock, and its floor set afresh at the next completion
    /// — a quiet device by design is not loss. Call before the device's
    /// output resumes.
    pub fn release(&mut self) -> Result<()> {
        let Some(at) = self.held_at.take() else {
            return Ok(());
        };
        if let Some(started) = self.started.as_mut() {
            *started += at.elapsed();
        }
        self.rebaseline = true;
        self.last_complete_us = 0;
        self.over_since = 0;
        self.fill_queue();
        Ok(())
    }

    /// Widen wire bytes into `buf`, pairing across calls via the carry
    /// byte: (samples written, bytes of `data` consumed). Never takes more
    /// than `buf` holds — the caller keeps the rest, in order, for the next
    /// call — and a trailing unpaired byte becomes the carry only once
    /// every pair before it fitted, so I and Q never trade places.
    fn widen_into(&mut self, data: &[u8], buf: &mut [Iq]) -> (usize, usize) {
        let (mut n, mut used) = (0usize, 0usize);
        if !buf.is_empty()
            && let Some(carry) = self.pending_byte
            && let Some(&q) = data.first()
        {
            buf[0] = self.fmt.widen(carry, q);
            self.pending_byte = None;
            n = 1;
            used = 1;
        }
        let pairs = ((data.len() - used) / 2).min(buf.len() - n);
        for (out, pair) in buf[n..n + pairs].iter_mut().zip(data[used..used + 2 * pairs].as_chunks::<2>().0) {
            *out = self.fmt.widen(pair[0], pair[1]);
        }
        n += pairs;
        used += 2 * pairs;
        if data.len() - used == 1 && self.pending_byte.is_none() {
            self.pending_byte = Some(data[used]);
            used += 1;
        }
        (n, used)
    }

    /// One chunk of the backlog (at most one transfer's worth), outside the
    /// ledger: these bytes were timed when they were taken.
    fn recv_backlog(&mut self, buf: &mut [Iq]) -> RecvInfo {
        let take = (self.backlog.len() - self.backlog_read).min(self.transfer_bytes);
        let backlog = core::mem::take(&mut self.backlog);
        let (n_samples, used) = self.widen_into(&backlog[self.backlog_read..self.backlog_read + take], buf);
        self.backlog = backlog;
        self.backlog_read += used;
        if self.backlog_read == self.backlog.len() {
            self.backlog.clear();
            self.backlog_read = 0;
        }
        self.delivered_bytes += 2 * n_samples as u64;
        RecvInfo { n_samples, ..Default::default() }
    }

    /// Copy the carry byte and then the backlog into `buf`; returns the
    /// bytes written.
    fn take_on_hand(&mut self, buf: &mut [u8]) -> usize {
        let mut n = 0;
        if !buf.is_empty()
            && let Some(b) = self.pending_byte.take()
        {
            buf[0] = b;
            n = 1;
        }
        let take = (self.backlog.len() - self.backlog_read).min(buf.len() - n);
        buf[n..n + take].copy_from_slice(&self.backlog[self.backlog_read..self.backlog_read + take]);
        self.backlog_read += take;
        if self.backlog_read == self.backlog.len() {
            self.backlog.clear();
            self.backlog_read = 0;
        }
        n += take;
        self.delivered_bytes += n as u64;
        n
    }

    pub fn max_num_samps(&self) -> usize {
        self.transfer_bytes / 2
    }

    pub fn start(&mut self) -> Result<()> {
        if self.started.is_none() {
            self.started = Some(Instant::now());
            self.last_complete_us = 0;
            self.completed = 0;
            self.delivered_bytes = 0;
            self.deficit_floor = 0.0;
            self.over_since = 0;
            self.fill_queue();
        }
        Ok(())
    }

    /// End the run: what is still on hand is discarded with it. Natively
    /// the queue is cancelled and drained so the endpoint is reusable; in a
    /// browser (no cancel) the queued transfers stay until the interface is
    /// released.
    pub fn stop(&mut self) -> Result<()> {
        self.backlog.clear();
        self.backlog_read = 0;
        self.held_at = None;
        self.rebaseline = false;
        if self.started.take().is_some() {
            #[cfg(not(target_arch = "wasm32"))]
            {
                self.endpoint.cancel_all();
                while self.endpoint.pending() > 0 {
                    if self.endpoint.wait_next_complete(CANCEL_WAIT).is_none() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Samples into `buf`, widened to sc16; at most `timeout_secs` waiting
    /// (`timed_out` then set).
    pub async fn recv_into_async(&mut self, buf: &mut [Iq], timeout_secs: f64) -> Result<RecvInfo> {
        if self.started.is_none() {
            self.start()?;
        }
        if self.backlog_read < self.backlog.len() {
            self.reap().await?;
            return Ok(self.recv_backlog(buf));
        }
        if self.held_at.is_some() {
            // Held, the backlog drained: nothing is queued until release.
            return Ok(RecvInfo { timed_out: true, ..Default::default() });
        }
        self.recv_completion(timeout_secs, |s, data| {
            // Widen into the caller's sc16 buffer, pairing across transfer
            // boundaries via the carry byte; what does not fit is kept,
            // in order, for the next call.
            let (n, used) = s.widen_into(data, buf);
            s.delivered_bytes += 2 * n as u64;
            s.backlog.extend_from_slice(&data[used..]);
            n
        })
        .await
    }

    /// [`BulkIqStream::recv_into_async`], blocking.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_into(&mut self, buf: &mut [Iq], timeout_secs: f64) -> Result<RecvInfo> {
        crate::block_on(self.recv_into_async(buf, timeout_secs))
    }

    /// The wire bytes as the device sent them, into `buf`: for an RTL2832U
    /// interleaved unsigned 8-bit I and Q, I first. Returns the bytes
    /// written (0 with `timed_out` set when nothing came in time) and the
    /// completion's report, whose `n_samples` is the bytes / 2. What does
    /// not fit `buf` is kept and handed out first by the next call, so any
    /// buffer size reads the stream whole. May be mixed with `recv_into`.
    pub async fn recv_bytes_async(&mut self, buf: &mut [u8], timeout_secs: f64) -> Result<(usize, RecvInfo)> {
        if self.started.is_none() {
            self.start()?;
        }
        if buf.is_empty() {
            return Ok((0, RecvInfo::default()));
        }
        if self.on_hand_bytes() > 0 {
            self.reap().await?;
            let n = self.take_on_hand(buf);
            return Ok((n, RecvInfo { n_samples: n / 2, ..Default::default() }));
        }
        if self.held_at.is_some() {
            return Ok((0, RecvInfo { timed_out: true, ..Default::default() }));
        }
        let mut written = 0usize;
        let info = self.recv_completion(timeout_secs, |s, data| {
            let k = data.len().min(buf.len());
            buf[..k].copy_from_slice(&data[..k]);
            s.delivered_bytes += k as u64;
            s.backlog.extend_from_slice(&data[k..]);
            written = k;
            k / 2
        })
        .await?;
        Ok((written, info))
    }

    /// [`BulkIqStream::recv_bytes_async`], blocking.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_bytes(&mut self, buf: &mut [u8], timeout_secs: f64) -> Result<(usize, RecvInfo)> {
        crate::block_on(self.recv_bytes_async(buf, timeout_secs))
    }

    /// Keep receiving for `span` without handing anything out: every
    /// completion's bytes go to the backlog (served first by the next
    /// receives), and the queue stays full. For a caller that must wait
    /// while the device streams (a backend holding a change until its
    /// output has settled) without letting the queue run dry.
    pub async fn absorb_async(&mut self, span: Duration) -> Result<()> {
        if self.started.is_none() || self.held_at.is_some() {
            return Ok(());
        }
        let until = Instant::now() + span;
        while let Some(left) = until.checked_duration_since(Instant::now()) {
            self.recv_completion(left.as_secs_f64(), |s, data| {
                s.backlog.extend_from_slice(data);
                0
            })
            .await?;
        }
        Ok(())
    }

    /// [`BulkIqStream::absorb_async`], blocking.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn absorb(&mut self, span: Duration) -> Result<()> {
        crate::block_on(self.absorb_async(span))
    }

    /// Wait for the next completion, hand its bytes to `consume` (which
    /// returns the samples it delivered), resubmit the buffer, and keep the
    /// ledger.
    async fn recv_completion(&mut self, timeout_secs: f64, consume: impl FnOnce(&mut Self, &[u8]) -> usize) -> Result<RecvInfo> {
        let started = self.started.expect("started by the caller");
        let wait_from_us = started.elapsed().as_micros() as u64;
        if self.endpoint.pending() == 0 {
            return Ok(RecvInfo { timed_out: true, ..Default::default() });
        }
        let Some(completion) = self.next_completion(Duration::from_secs_f64(timeout_secs.max(0.0))).await else {
            return Ok(RecvInfo {
                timed_out: true,
                ..Default::default()
            });
        };
        self.process(completion, wait_from_us, consume).await
    }

    /// Every transfer already complete, taken into the backlog and
    /// resubmitted, without waiting. Called before serving the backlog: a
    /// consumer working through a large backlog (after a hold, or a wait
    /// that kept receiving) does not take completions from the queue, and
    /// left alone the queue runs dry and the device drops (findings 16.9:
    /// a pile of back-to-back breaks then lost a whole break's worth).
    ///
    /// At most one queue's worth per call: a device producing as fast as
    /// the host can take completions would otherwise keep the reaping
    /// going for ever and the backlog never served.
    async fn reap(&mut self) -> Result<()> {
        if self.started.is_none() || self.held_at.is_some() {
            return Ok(());
        }
        let waker = core::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(waker);
        // The bound was A/B'd against the old unbounded loop on the bench
        // (2026-10-11, tiercheck at 1.2 Msps, 7 interleaved runs each):
        // phase jumps 1,371 vs 1,367 mean, 0 blocks dropped in every run.
        for _ in 0..self.queue_depth {
            if self.endpoint.pending() == 0 {
                break;
            }
            let core::task::Poll::Ready(c) = self.endpoint.poll_next_complete(&mut cx) else {
                break;
            };
            let wait_from_us = self.started.map(|s| s.elapsed().as_micros() as u64).unwrap_or(0);
            self.process(c, wait_from_us, |s, data| {
                s.backlog.extend_from_slice(data);
                0
            })
            .await?;
        }
        Ok(())
    }

    /// One completion: its bytes to `consume`, the buffer resubmitted, the
    /// ledger kept.
    async fn process(&mut self, completion: Completion, wait_from_us: u64, consume: impl FnOnce(&mut Self, &[u8]) -> usize) -> Result<RecvInfo> {
        let started = self.started.expect("started by the caller");
        let now_us = started.elapsed().as_micros() as u64;

        // Timing forensics. `gap` (inter-completion spacing at dequeue) and
        // `late` are kept as observability, but loss is NOT computed from
        // them — a gap can be consumer lag whose data the transfer queue
        // faithfully buffered (no loss), and gap arithmetic double-counts
        // that buffered data. Loss is a *ledger* instead (below).
        let expected_us = (self.transfer_bytes as f64 / 2.0) / self.sample_rate * 1e6;
        let gap_us = now_us.saturating_sub(self.last_complete_us);
        let blocked_us = now_us.saturating_sub(wait_from_us);
        self.completed += 1;
        let late = self.last_complete_us > 0 && (gap_us as f64) > expected_us * self.gap_factor;
        self.last_complete_us = now_us;

        let n_samples = consume(self, &completion.buffer[..completion.actual_len]);

        match completion.status {
            Ok(()) | Err(TransferError::Cancelled) => {
                // Recycle the buffer into a fresh submission immediately.
                let mut resubmit = completion.buffer;
                resubmit.clear();
                resubmit.set_requested_len(self.transfer_bytes);
                self.endpoint.submit(resubmit);
            }
            Err(TransferError::Stall) => self.recover_stall().await?,
            Err(e) => {
                return Err(Error::Usb {
                    backend: self.backend,
                    context: "bulk recv",
                    message: transfer_message(e, self.transfer_bytes, self.queue_depth),
                });
            }
        }

        // The sample ledger. The device's clock produces `rate` samples per
        // second whether or not they reach us, so
        //     deficit = elapsed × rate − samples taken off the wire
        // (handed out, or on hand for the next call) is the ground truth of
        // host-side loss, measured against a USB analyzer's wiretap to
        // 0.1%: consumer stalls the queue absorbed pay
        // the deficit back when the buffered completions drain (no false
        // positive), and stalls that overran the queue — or the device
        // going quiet — leave it permanently raised (no blind spot). The
        // floor creeps toward the deficit while the margin is small,
        // absorbing sample-clock-vs-wall-clock drift (~10⁻⁴/s of a
        // transfer) but never a step of half a transfer or more.
        let taken = (self.delivered_bytes + self.on_hand_bytes()) as f64 / 2.0;
        let deficit = (now_us as f64 / 1e6) * self.sample_rate - taken;
        if self.completed == 1 || self.rebaseline {
            self.rebaseline = false;
            self.deficit_floor = deficit;
        }
        let threshold = 1.5 * self.max_num_samps() as f64;
        let queue_cap = (self.queue_depth * self.max_num_samps()) as f64;
        let margin = deficit - self.deficit_floor;
        let mut inferred_lost = 0u64;
        if margin > threshold + queue_cap {
            // Saturation backstop: the queue cannot be holding more than
            // its own capacity, so deficit beyond it is certainly lost —
            // a consumer that stays slower than the sample clock, or a
            // stall past the queue's depth with the consumer still busy.
            inferred_lost = (margin - queue_cap) as u64;
            self.deficit_floor = deficit - queue_cap;
            self.over_since = 0;
        } else if margin > threshold {
            // Don't judge yet: this deficit may be samples *held in the
            // queue* (a consumer running transiently behind, or the burst
            // right after an absorbed stall) which pay it back when they
            // drain. Only dequeues that actually blocked on the device —
            // stream running, margin still high — advance the fuse; a
            // non-blocking dequeue means data was waiting, so the backlog
            // is queue-held and judgment waits.
            if blocked_us as f64 > expected_us * 0.5 {
                self.over_since += 1;
            }
            if self.over_since > self.queue_depth {
                inferred_lost = margin as u64;
                self.deficit_floor = deficit;
                self.over_since = 0;
            }
        } else {
            self.over_since = 0;
            if margin < -threshold {
                // A deficit far below the floor can't be physical (clocks
                // drift slowly): the floor was left high by a transient —
                // snap it down so later loss is measured from reality.
                self.deficit_floor = deficit;
            } else if margin.abs() < threshold * 0.5 {
                self.deficit_floor += 0.001 * margin;
            }
        }

        self.lost_total += inferred_lost;
        Ok(RecvInfo {
            n_samples,
            overflow: inferred_lost > 0,
            timed_out: false,
            out_of_sequence: false,
            more_fragments: false,
            time_spec: None,
            link: Some(LinkStats {
                transfers: 1,
                max_gap_us: gap_us.min(u32::MAX as u64) as u32,
                late_transfers: late as u32,
                inferred_lost_samples: inferred_lost,
                ledger_deficit_samples: deficit as i64,
                completed_at_us: now_us,
            }),
        })
    }
}

impl Drop for BulkIqStream {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::mock::{self, MockHandle};
    use super::*;

    /// Transfer size the tests use: 32 packets of 512.
    const TRANSFER: usize = 16_384;
    const QUEUE: usize = 4;

    fn stream() -> (BulkIqStream, MockHandle) {
        let (ep, handle) = mock::endpoint();
        (BulkIqStream::with_endpoint("mock", Box::new(ep), WireSample::OffsetU8, TRANSFER, QUEUE, 2.4e6), handle)
    }

    /// `n` samples whose I and Q bytes say where they are: I = k mod 256,
    /// Q = (k mod 256) ^ 0x5A, so a swap or a lost byte shows.
    fn wire(n: usize) -> Vec<u8> {
        (0..n).flat_map(|k| [k as u8, (k as u8) ^ 0x5A]).collect()
    }

    fn expected(n: usize) -> Vec<Iq> {
        let w = wire(n);
        w.as_chunks::<2>().0.iter().map(|p| WireSample::OffsetU8.widen(p[0], p[1])).collect()
    }

    /// Every sample, through `recv_into` with buffers of `size`, until the
    /// device has nothing more.
    fn drain(s: &mut BulkIqStream, size: usize) -> Vec<Iq> {
        let mut out = Vec::new();
        let mut buf = vec![Iq::default(); size];
        loop {
            let info = s.recv_into(&mut buf, 0.0).expect("recv");
            if info.timed_out {
                break;
            }
            out.extend_from_slice(&buf[..info.n_samples]);
        }
        out
    }

    /// Review A2: a buffer smaller than a transfer, or not a divisor of
    /// it, and transfers of odd and uneven lengths: every sample arrives,
    /// in order, I and Q in place.
    #[test]
    fn every_buffer_size_keeps_every_sample_in_order() {
        let n = 50_000;
        let w = wire(n);
        // Completions of awkward lengths: odd, one byte, a whole transfer,
        // more than a transfer (split by the endpoint).
        let cuts = [16_383, 1, 16_384, 7, 20_001, 3, 4_096];
        for size in [1, 2, 3, 7, 1_000, 8_191, 8_192, 8_193, 100_000] {
            let (mut s, h) = stream();
            let mut at = 0;
            for &c in cuts.iter().cycle() {
                if at >= w.len() {
                    break;
                }
                let end = (at + c).min(w.len());
                h.send(w[at..end].to_vec());
                at = end;
            }
            let got = drain(&mut s, size);
            assert_eq!(got.len(), n, "buffer {size}: samples");
            assert!(got == expected(n), "buffer {size}: a sample out of order or I/Q swapped");
            assert_eq!(s.delivered(), n as u64);
        }
    }

    /// The wire bytes, any buffer size, mixed with `recv_into`: the same
    /// bytes in the same order.
    #[test]
    fn bytes_and_samples_mixed_keep_order() {
        let (mut s, h) = stream();
        let w = wire(30_000);
        h.send(w[..16_383].to_vec());
        h.send(w[16_383..].to_vec());
        let mut got: Vec<u8> = Vec::new();
        let mut bytes = [0u8; 999];
        let mut iq = vec![Iq::default(); 777];
        for turn in 0.. {
            if turn % 2 == 0 {
                let (n, info) = s.recv_bytes(&mut bytes, 0.0).unwrap();
                if info.timed_out {
                    break;
                }
                got.extend_from_slice(&bytes[..n]);
            } else {
                // recv_into hands out whole samples; a carry byte from an
                // odd recv_bytes count pairs with the next.
                let info = s.recv_into(&mut iq, 0.0).unwrap();
                if info.timed_out {
                    break;
                }
                for x in &iq[..info.n_samples] {
                    got.push(((x.i >> 8) + 128) as u8);
                    got.push(((x.q >> 8) + 128) as u8);
                }
            }
        }
        // Whatever recv_bytes took as a lone byte returns as half a sample.
        assert_eq!(&got[..], &w[..got.len()]);
        assert!(w.len() - got.len() <= 1, "{} of {} bytes", got.len(), w.len());
    }

    /// Review A6: a stall mid-stream. The queue is cancelled and drained,
    /// the halt cleared with nothing pending, the queue refilled; every
    /// byte before and after arrives in order.
    #[test]
    fn a_stall_is_cleared_with_nothing_pending_and_the_stream_goes_on() {
        let (mut s, h) = stream();
        let w = wire(20_000);
        h.send(w[..10_000].to_vec());
        h.fail(TransferError::Stall, w[10_000..12_000].to_vec());
        h.send(w[12_000..].to_vec());
        let got = drain(&mut s, 4_096);
        assert!(got == expected(20_000), "order across the stall");
        let st = h.stats();
        assert_eq!(st.clear_halts, 1);
        assert_eq!(st.clear_halts_with_pending, 0, "nusb: never clear a halt with transfers pending");
        assert_eq!(st.cancels, 1);
        assert_eq!(h.pending(), QUEUE, "the queue refilled");
    }

    /// A halt that cannot be cleared ends the stream with an error, not a
    /// stream that only ever times out.
    #[test]
    fn a_halt_that_cannot_be_cleared_is_an_error() {
        let (mut s, h) = stream();
        h.send(wire(100));
        h.fail(TransferError::Stall, Vec::new());
        h.fail_next_clear_halt("pipe gone");
        let mut buf = vec![Iq::default(); 8_192];
        assert!(s.recv_into(&mut buf, 0.0).is_ok());
        let e = s.recv_into(&mut buf, 0.0).expect_err("the failed clear");
        assert!(e.to_string().contains("pipe gone"), "{e}");
    }

    /// Any bulk error but a stall or a cancel ends the stream with it.
    #[test]
    fn a_fault_is_an_error() {
        let (mut s, h) = stream();
        h.fail(TransferError::Fault, Vec::new());
        let mut buf = vec![Iq::default(); 8_192];
        assert!(s.recv_into(&mut buf, 0.0).is_err());
    }

    /// Linux's ENOMEM from usbfs names the limit to raise.
    #[cfg(target_os = "linux")]
    #[test]
    fn usbfs_out_of_memory_names_the_limit() {
        let (mut s, h) = stream();
        h.fail(TransferError::Unknown(LINUX_ENOMEM), Vec::new());
        let mut buf = vec![Iq::default(); 8_192];
        let e = s.recv_into(&mut buf, 0.0).expect_err("ENOMEM ends the stream").to_string();
        assert!(e.contains("usbfs_memory_mb"), "{e}");
    }

    /// A hold takes the queue back (cancelled, nothing lost), reports the
    /// break at the samples handed out plus on hand; after the release the
    /// stream resumes in order and the ledger starts afresh.
    #[test]
    fn hold_and_release_keep_order_and_place_the_break() {
        let (mut s, h) = stream();
        let w = wire(40_000);
        h.send(w[..30_001].to_vec());
        let mut buf = vec![Iq::default(); 5_000];
        let first = s.recv_into(&mut buf, 0.0).unwrap();
        let mut got: Vec<Iq> = buf[..first.n_samples].to_vec();
        // The device pauses: no more of the script plays until release.
        let at = s.hold().unwrap();
        assert!(s.is_held());
        assert_eq!(h.pending(), 0, "every queued transfer taken back");
        // Everything taken off the wire before the hold comes out first.
        got.extend(drain(&mut s, 3_000));
        assert_eq!(got.len() as u64, at, "the break sits after what was on the wire");
        s.release().unwrap();
        assert_eq!(h.pending(), QUEUE);
        h.send(w[30_001..].to_vec());
        got.extend(drain(&mut s, 3_000));
        assert!(got == expected(40_000), "order across the hold");
    }

    /// Nothing queued and nothing on hand: a receive times out at once
    /// rather than waiting on a transfer that will never come.
    #[test]
    fn an_empty_device_times_out() {
        let (mut s, _h) = stream();
        let mut buf = vec![Iq::default(); 10];
        assert!(s.recv_into(&mut buf, 0.0).unwrap().timed_out);
    }
}
