use std::ops::Deref;
use std::pin::Pin;
use std::vec::Vec;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use crate::Iq;
use crate::error::Result;
use crate::ring::Ring;
use crate::source::RxSource;
use crate::types::RecvInfo;

/// One received buffer, on loan from the stream's pool; derefs to `[Iq]`
/// (already sized to the sample count). Dropping it returns the buffer.
pub struct IqChunk {
    data: Vec<Iq>,
    ring: Arc<Ring>,
    pub info: RecvInfo,
    /// Chunks discarded between the previous chunk the consumer saw and this
    /// one because the consumer fell behind. 0 means nothing was lost.
    pub dropped_before: u64,
}

impl Deref for IqChunk {
    type Target = [Iq];
    fn deref(&self) -> &[Iq] {
        &self.data
    }
}

impl Drop for IqChunk {
    fn drop(&mut self) {
        self.ring.recycle(std::mem::take(&mut self.data));
    }
}

/// Async handle to a continuous RX stream: a dedicated thread runs the
/// blocking `recv` loop, and this end implements `futures_core::Stream`
/// under any executor. Dropping it stops and joins the worker, which stops
/// the stream and frees the handles.
pub struct IqStream {
    ring: Arc<Ring>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl IqStream {
    /// Start continuous streaming and move `source` onto a worker thread.
    /// `depth` bounds the queued chunks; if the consumer falls behind, the
    /// oldest queued chunk is dropped (and counted on the next chunk's
    /// `dropped_before`) rather than stalling `recv` — a stalled `recv`
    /// would surface as unaccounted device-side overflows instead.
    pub fn spawn(mut source: impl RxSource, depth: usize) -> Result<IqStream> {
        let depth = depth.max(1);
        let max_samps = source.max_num_samps();
        source.start()?;
        let ring = Arc::new(Ring::new(depth, max_samps));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let ring = Arc::clone(&ring);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                // Best-effort realtime elevation. Without it, every UI
                // interaction's "user-interactive" scheduler boost can
                // preempt the recv loop long enough to overflow the USB
                // transport; streaming works without it, just with more
                // overflows under desktop load.
                source.setup_worker_thread();
                while !stop.load(Ordering::Relaxed) {
                    let mut buf = ring.take_buffer();
                    buf.resize(source.max_num_samps(), Iq::default());
                    // 1 s timeout bounds how long IqStream::drop can wait on
                    // join. Timeout chunks (0 samples) are pushed like any
                    // other so the consumer stays informed.
                    match source.recv_into(&mut buf, 1.0) {
                        Ok(info) => {
                            buf.truncate(info.n_samples);
                            if let Some(w) = ring.push(buf, info) {
                                w.wake();
                            }
                        }
                        Err(e) => {
                            ring.recycle(buf);
                            if let Some(w) = ring.fail(e) {
                                w.wake();
                            }
                            break;
                        }
                    }
                }
                // The source drops here, on the worker thread: the stop
                // command and handle frees happen before `finished` shows.
                drop(source);
                if let Some(w) = ring.finish() {
                    w.wake();
                }
            })
        };
        Ok(IqStream {
            ring,
            stop,
            worker: Some(worker),
        })
    }
}

impl futures_core::Stream for IqStream {
    type Item = Result<IqChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.ring.pop(cx).map(|opt| {
            opt.map(|res| {
                res.map(|slot| IqChunk {
                    data: slot.data,
                    ring: Arc::clone(&this.ring),
                    info: slot.info,
                    dropped_before: slot.dropped_before,
                })
            })
        })
    }
}

impl Drop for IqStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
