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

use core::time::Duration;
use std::string::ToString;
use std::vec::Vec;

use nusb::transfer::{Bulk, Completion, In, TransferError};

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

/// A running bulk RX queue. One `recv_into` call consumes one completion;
/// 8-bit wire samples widen into the caller's sc16 buffer during the copy.
pub struct BulkIqStream {
    backend: &'static str,
    endpoint: nusb::Endpoint<Bulk, In>,
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
        }
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
        while self.endpoint.pending() > 0 {
            let Some(c) = self.next_completion(wait).await else {
                break;
            };
            match c.status {
                Ok(()) | Err(TransferError::Cancelled) => {}
                Err(TransferError::Stall) => self.clear_halt().await,
                Err(e) => {
                    return Err(Error::Usb {
                        backend: self.backend,
                        context: "bulk hold",
                        message: e.to_string(),
                    });
                }
            }
            self.backlog.extend_from_slice(&c.buffer[..c.actual_len]);
        }
        self.held_at = Some(Instant::now());
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

    /// Clear a stalled endpoint (the stall itself shows up as a gap).
    async fn clear_halt(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        let _ = nusb::MaybeFuture::wait(self.endpoint.clear_halt());
        #[cfg(target_arch = "wasm32")]
        let _ = self.endpoint.clear_halt().await;
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
        while self.endpoint.pending() < self.queue_depth {
            let mut buf = self.endpoint.allocate(self.transfer_bytes);
            buf.set_requested_len(self.transfer_bytes);
            self.endpoint.submit(buf);
        }
        Ok(())
    }

    /// Widen wire bytes into `buf`, pairing across calls via the carry
    /// byte; returns the samples written.
    fn widen_into(&mut self, data: &[u8], buf: &mut [Iq]) -> usize {
        let mut n_samples = 0usize;
        let mut iter = data.iter().copied();
        if let Some(carry) = self.pending_byte.take()
            && let Some(q) = iter.next()
        {
            buf[0] = self.fmt.widen(carry, q);
            n_samples = 1;
        }
        while let Some(i) = iter.next() {
            let Some(q) = iter.next() else {
                self.pending_byte = Some(i);
                break;
            };
            if n_samples >= buf.len() {
                // Cannot happen while buf is sized to max_num_samps (the
                // carry byte offsets exactly); safety valve regardless.
                self.pending_byte = Some(i);
                break;
            }
            buf[n_samples] = self.fmt.widen(i, q);
            n_samples += 1;
        }
        n_samples
    }

    /// One chunk of the backlog (at most one transfer's worth), outside the
    /// ledger: these bytes were timed when they were taken.
    fn recv_backlog(&mut self, buf: &mut [Iq]) -> RecvInfo {
        let take = (self.backlog.len() - self.backlog_read).min(self.transfer_bytes).min(buf.len() * 2);
        let chunk: Vec<u8> = self.backlog[self.backlog_read..self.backlog_read + take].to_vec();
        self.backlog_read += take;
        if self.backlog_read == self.backlog.len() {
            self.backlog.clear();
            self.backlog_read = 0;
        }
        let n_samples = self.widen_into(&chunk, buf);
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
            while self.endpoint.pending() < self.queue_depth {
                let mut buf = self.endpoint.allocate(self.transfer_bytes);
                buf.set_requested_len(self.transfer_bytes);
                self.endpoint.submit(buf);
            }
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
            // boundaries via the carry byte.
            let n = s.widen_into(data, buf);
            s.delivered_bytes += 2 * n as u64;
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
    async fn reap(&mut self) -> Result<()> {
        if self.started.is_none() || self.held_at.is_some() {
            return Ok(());
        }
        let waker = core::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(waker);
        while self.endpoint.pending() > 0 {
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

        // Recycle the buffer into a fresh submission immediately.
        let mut resubmit = completion.buffer;
        resubmit.clear();
        resubmit.set_requested_len(self.transfer_bytes);
        self.endpoint.submit(resubmit);

        match completion.status {
            Ok(()) => {}
            Err(TransferError::Cancelled) => {}
            Err(TransferError::Stall) => self.clear_halt().await,
            Err(e) => {
                return Err(Error::Usb {
                    backend: self.backend,
                    context: "bulk recv",
                    message: e.to_string(),
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
