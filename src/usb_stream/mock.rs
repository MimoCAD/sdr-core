//! A scripted bulk IN endpoint: the device's side of a [`BulkIqStream`]
//! played from a script, for tests that cannot be run on hardware —
//! stalls, faults, cancels, short and odd-length transfers, a device that
//! goes quiet. Native only (the engine's waits there are nusb's blocking
//! ones; here they return at once).
//!
//! The test keeps a [`MockHandle`] to push what the device sends and to
//! read what the engine did ([`Stats`]): every submit, cancel and halt
//! clear, and how many transfers were pending when a halt was cleared
//! (nusb: never with transfers pending).
//!
//! [`BulkIqStream`]: super::BulkIqStream

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::boxed::Box;
use std::collections::VecDeque;
use std::string::String;
use std::sync::{Arc, Mutex, MutexGuard};
use std::vec::Vec;

use nusb::transfer::{Buffer, Completion, TransferError};

use super::BulkIn;

/// What the device does with the next queued transfer.
#[derive(Clone, Debug)]
pub enum Event {
    /// Sends these bytes. More than the transfer asked for are split: the
    /// rest fills the next transfer, as on the wire.
    Data(Vec<u8>),
    /// Ends the transfer with this error after these bytes. A stall halts
    /// the endpoint: nothing more completes until the halt is cleared.
    Fail(TransferError, Vec<u8>),
}

/// What the engine did to the endpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub submitted: u64,
    pub completed: u64,
    /// `cancel_all` calls.
    pub cancels: u64,
    pub clear_halts: u64,
    /// Halt clears made with transfers still pending (nusb forbids it).
    pub clear_halts_with_pending: u64,
    /// The most transfers pending at once.
    pub max_pending: usize,
}

struct Queued {
    buf: Buffer,
    cancelled: bool,
}

/// What the device sends once the script is played out, asked per
/// transfer with the bytes the transfer wants; `None` sends nothing (the
/// device quiet).
pub type Source = Box<dyn FnMut(usize) -> Option<Event> + Send>;

#[derive(Default)]
struct State {
    script: VecDeque<Event>,
    source: Option<Source>,
    queue: VecDeque<Queued>,
    halted: bool,
    /// Results for the next halt clears; `Ok` when empty.
    clear_halt_results: VecDeque<Result<(), String>>,
    stats: Stats,
}

/// The endpoint the engine owns.
pub struct MockBulkIn {
    state: Arc<Mutex<State>>,
    max_packet_size: usize,
}

/// The test's side: script the device, read the stats.
#[derive(Clone)]
pub struct MockHandle {
    state: Arc<Mutex<State>>,
}

/// A new endpoint and its handle; 512-byte packets, as a high-speed bulk
/// endpoint.
pub fn endpoint() -> (MockBulkIn, MockHandle) {
    let state = Arc::new(Mutex::new(State::default()));
    (
        MockBulkIn {
            state: state.clone(),
            max_packet_size: 512,
        },
        MockHandle { state },
    )
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(|p| p.into_inner())
}

impl MockHandle {
    /// The device sends these bytes.
    pub fn send(&self, bytes: impl Into<Vec<u8>>) {
        lock(&self.state)
            .script
            .push_back(Event::Data(bytes.into()));
    }

    /// The device ends a transfer with `error` after `bytes`.
    pub fn fail(&self, error: TransferError, bytes: impl Into<Vec<u8>>) {
        lock(&self.state)
            .script
            .push_back(Event::Fail(error, bytes.into()));
    }

    /// What the device sends after the script: asked for each transfer.
    pub fn set_source(&self, source: impl FnMut(usize) -> Option<Event> + Send + 'static) {
        lock(&self.state).source = Some(Box::new(source));
    }

    /// The next halt clear fails with `message`.
    pub fn fail_next_clear_halt(&self, message: &str) {
        lock(&self.state)
            .clear_halt_results
            .push_back(Err(message.into()));
    }

    pub fn stats(&self) -> Stats {
        lock(&self.state).stats.clone()
    }

    /// Script events the device has not played yet.
    pub fn unplayed(&self) -> usize {
        lock(&self.state).script.len()
    }

    pub fn pending(&self) -> usize {
        lock(&self.state).queue.len()
    }
}

impl State {
    /// The next completion, if the device has one to give.
    fn next(&mut self) -> Option<Completion> {
        // Cancelled transfers complete first, empty.
        if let Some(i) = self.queue.iter().position(|q| q.cancelled) {
            let q = self.queue.remove(i).expect("position is in range");
            self.stats.completed += 1;
            return Some(Completion {
                buffer: q.buf,
                actual_len: 0,
                status: Err(TransferError::Cancelled),
            });
        }
        if self.halted {
            return None;
        }
        let want = self.queue.front()?.buf.requested_len();
        let event = match self.script.pop_front() {
            Some(e) => e,
            None => (self.source.as_mut()?)(want)?,
        };
        let mut q = self.queue.pop_front().expect("checked non-empty");
        let (bytes, status) = match event {
            Event::Data(b) => (b, Ok(())),
            Event::Fail(e, b) => (b, Err(e)),
        };
        let take = bytes.len().min(want);
        if bytes.len() > want {
            // The rest goes in the next transfer; a failure ends that one.
            let rest = bytes[take..].to_vec();
            self.script.push_front(match status {
                Ok(()) => Event::Data(rest),
                Err(e) => Event::Fail(e, rest),
            });
        } else if status == Err(TransferError::Stall) {
            self.halted = true;
        }
        let status = if bytes.len() > want { Ok(()) } else { status };
        q.buf.clear();
        q.buf.extend_from_slice(&bytes[..take]);
        self.stats.completed += 1;
        Some(Completion {
            buffer: q.buf,
            actual_len: take,
            status,
        })
    }
}

impl BulkIn for MockBulkIn {
    fn max_packet_size(&self) -> usize {
        self.max_packet_size
    }

    fn allocate(&self, len: usize) -> Buffer {
        Buffer::new(len)
    }

    fn submit(&mut self, buf: Buffer) {
        let mut s = lock(&self.state);
        s.queue.push_back(Queued {
            buf,
            cancelled: false,
        });
        s.stats.submitted += 1;
        s.stats.max_pending = s.stats.max_pending.max(s.queue.len());
    }

    fn pending(&self) -> usize {
        lock(&self.state).queue.len()
    }

    fn cancel_all(&mut self) {
        let mut s = lock(&self.state);
        s.stats.cancels += 1;
        for q in &mut s.queue {
            q.cancelled = true;
        }
    }

    fn poll_next_complete(&mut self, _cx: &mut Context<'_>) -> Poll<Completion> {
        match lock(&self.state).next() {
            Some(c) => Poll::Ready(c),
            None => Poll::Pending,
        }
    }

    /// Nothing ready: one short nap (at most the timeout, 1 ms), then one
    /// more look — enough for a paced source, without a test spinning hot.
    fn wait_next_complete(&mut self, timeout: Duration) -> Option<Completion> {
        if let Some(c) = lock(&self.state).next() {
            return Some(c);
        }
        if timeout.is_zero() {
            return None;
        }
        std::thread::sleep(timeout.min(Duration::from_millis(1)));
        lock(&self.state).next()
    }

    fn clear_halt(&mut self) -> Pin<Box<dyn Future<Output = Result<(), String>> + '_>> {
        let mut s = lock(&self.state);
        s.stats.clear_halts += 1;
        if !s.queue.is_empty() {
            s.stats.clear_halts_with_pending += 1;
        }
        let r = s.clear_halt_results.pop_front().unwrap_or(Ok(()));
        if r.is_ok() {
            s.halted = false;
        }
        Box::pin(core::future::ready(r))
    }
}
