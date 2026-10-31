use std::collections::VecDeque;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::vec::Vec;

use crate::Iq;
use crate::error::{Error, Result};
use crate::types::RecvInfo;

/// One filled buffer, what the metadata said about it, and how many chunks
/// were dropped between it and the previous one the consumer saw.
pub(crate) struct Slot {
    pub data: Vec<Iq>,
    pub info: RecvInfo,
    pub dropped_before: u64,
}

struct State {
    queue: VecDeque<Slot>,
    free: Vec<Vec<Iq>>,
    waker: Option<Waker>,
    fatal: Option<Error>,
    finished: bool,
    // Chunks discarded since the last slot queued, folded into the next
    // slot's `dropped_before` so no loss goes unaccounted.
    dropped: u64,
}

/// Bounded drop-oldest queue with a recycled-buffer pool. The producer never
/// blocks — that's the point: `recv` stays hardware-paced, so consumer lag
/// shows up as counted drops here instead of unaccounted device overflows.
/// The consumer parks on a `Waker`. No FFI, so it tests without a radio.
pub(crate) struct Ring {
    depth: usize,
    state: Mutex<State>,
}

impl Ring {
    /// `depth` must be >= 1 (the producer relies on eviction succeeding).
    pub fn new(depth: usize, buf_capacity: usize) -> Ring {
        Ring {
            depth,
            state: Mutex::new(State {
                queue: VecDeque::with_capacity(depth),
                free: (0..depth)
                    .map(|_| Vec::with_capacity(buf_capacity))
                    .collect(),
                waker: None,
                fatal: None,
                finished: false,
                dropped: 0,
            }),
        }
    }

    /// Producer: a buffer to fill — recycled if available, else reclaimed
    /// from the oldest queued slot (counted as dropped), else fresh.
    pub fn take_buffer(&self) -> Vec<Iq> {
        let mut st = self.state.lock().unwrap();
        if let Some(buf) = st.free.pop() {
            return buf;
        }
        if let Some(old) = st.queue.pop_front() {
            st.dropped += 1 + old.dropped_before;
            let mut buf = old.data;
            buf.clear();
            return buf;
        }
        Vec::new()
    }

    /// Producer: queue a filled buffer, evicting the oldest if full.
    /// Returns the waker so the caller can wake *outside* the lock.
    #[must_use]
    pub fn push(&self, data: Vec<Iq>, info: RecvInfo) -> Option<Waker> {
        let mut st = self.state.lock().unwrap();
        if st.queue.len() >= self.depth {
            let old = st.queue.pop_front().unwrap();
            st.dropped += 1 + old.dropped_before;
            let mut buf = old.data;
            buf.clear();
            st.free.push(buf);
        }
        let dropped_before = std::mem::take(&mut st.dropped);
        st.queue.push_back(Slot {
            data,
            info,
            dropped_before,
        });
        st.waker.take()
    }

    /// Producer: record a fatal error and end the stream (queued slots still
    /// drain first; the error is yielded after them, then `None`).
    #[must_use]
    pub fn fail(&self, e: Error) -> Option<Waker> {
        let mut st = self.state.lock().unwrap();
        st.fatal.get_or_insert(e);
        st.finished = true;
        st.waker.take()
    }

    #[must_use]
    pub fn finish(&self) -> Option<Waker> {
        let mut st = self.state.lock().unwrap();
        st.finished = true;
        st.waker.take()
    }

    /// Consumer side of `poll_next`: queued slot → fatal error → end → Pending.
    pub fn pop(&self, cx: &Context<'_>) -> Poll<Option<Result<Slot>>> {
        let mut st = self.state.lock().unwrap();
        if let Some(slot) = st.queue.pop_front() {
            return Poll::Ready(Some(Ok(slot)));
        }
        if let Some(e) = st.fatal.take() {
            return Poll::Ready(Some(Err(e)));
        }
        if st.finished {
            return Poll::Ready(None);
        }
        match &st.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => st.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    /// Dropped `IqChunk`s return their buffers here.
    pub fn recycle(&self, mut buf: Vec<Iq>) {
        buf.clear();
        self.state.lock().unwrap().free.push(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    struct Flag(AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn test_cx() -> (Arc<Flag>, Waker) {
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        (flag, waker)
    }

    fn tagged(tag: i16) -> Vec<Iq> {
        vec![Iq { i: tag, q: 0 }]
    }

    fn pop_slot(ring: &Ring, cx: &Context<'_>) -> Slot {
        match ring.pop(cx) {
            Poll::Ready(Some(Ok(slot))) => slot,
            other => panic!(
                "expected a slot, got {:?}",
                other.map(|o| o.map(|r| r.is_ok()))
            ),
        }
    }

    #[test]
    fn drop_oldest_with_accounting() {
        let ring = Ring::new(2, 4);
        assert!(ring.push(tagged(1), RecvInfo::default()).is_none());
        assert!(ring.push(tagged(2), RecvInfo::default()).is_none());
        // Full: this evicts slot 1, and the drop is charged to slot 3.
        assert!(ring.push(tagged(3), RecvInfo::default()).is_none());

        let (_, waker) = test_cx();
        let cx = Context::from_waker(&waker);
        let a = pop_slot(&ring, &cx);
        assert_eq!((a.data[0].i, a.dropped_before), (2, 0));
        let b = pop_slot(&ring, &cx);
        assert_eq!((b.data[0].i, b.dropped_before), (3, 1));
        assert!(matches!(ring.pop(&cx), Poll::Pending));
    }

    #[test]
    fn starved_producer_steals_oldest() {
        let ring = Ring::new(2, 0);
        // Drain the pre-filled pool so take_buffer must steal.
        let _a = ring.take_buffer();
        let _b = ring.take_buffer();
        assert!(ring.push(tagged(1), RecvInfo::default()).is_none());
        let buf = ring.take_buffer(); // steals (and clears) slot 1
        assert!(buf.is_empty());
        assert!(ring.push(buf, RecvInfo::default()).is_none());

        let (_, waker) = test_cx();
        let cx = Context::from_waker(&waker);
        assert_eq!(pop_slot(&ring, &cx).dropped_before, 1);
    }

    #[test]
    fn wake_on_push_then_finish_ends_stream() {
        let ring = Ring::new(2, 0);
        let (flag, waker) = test_cx();
        let cx = Context::from_waker(&waker);
        assert!(matches!(ring.pop(&cx), Poll::Pending));
        if let Some(w) = ring.push(Vec::new(), RecvInfo::default()) {
            w.wake();
        }
        assert!(flag.0.load(Ordering::SeqCst));
        let _ = pop_slot(&ring, &cx);
        assert!(matches!(ring.pop(&cx), Poll::Pending));
        if let Some(w) = ring.finish() {
            w.wake();
        }
        assert!(matches!(ring.pop(&cx), Poll::Ready(None)));
    }

    #[test]
    fn fatal_error_yields_once_then_none() {
        let ring = Ring::new(1, 0);
        let _ = ring.fail(Error::Device {
            backend: "test",
            context: "test",
            message: "boom".into(),
        });
        let (_, waker) = test_cx();
        let cx = Context::from_waker(&waker);
        assert!(matches!(ring.pop(&cx), Poll::Ready(Some(Err(_)))));
        assert!(matches!(ring.pop(&cx), Poll::Ready(None)));
    }

    #[test]
    fn recycle_preserves_capacity() {
        let ring = Ring::new(1, 0);
        let mut buf = ring.take_buffer();
        buf.reserve(1024);
        let cap = buf.capacity();
        ring.recycle(buf);
        assert!(ring.take_buffer().capacity() >= cap);
    }
}
