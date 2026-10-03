//! What a driver's async core needs to be shared and driven: an async
//! lock (two halves of one stream, on two threads natively or two tasks in
//! a browser) and, natively, [`block_on`] for the blocking wrappers.

use core::cell::UnsafeCell;
use core::future::poll_fn;
use core::ops::{Deref, DerefMut};
use core::task::{Poll, Waker};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;

/// Run a future to completion on this thread, parking it while the future
/// waits. The drivers' native async code completes synchronously except
/// where it waits on another thread (an async [`Lock`] held elsewhere);
/// no runtime is needed.
#[cfg(not(target_arch = "wasm32"))]
pub fn block_on<F: core::future::Future>(f: F) -> F::Output {
    use core::pin::pin;
    use core::task::Context;
    use std::task::Wake;

    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut f = pin!(f);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park(),
        }
    }
}

struct State {
    locked: bool,
    waiting: Vec<Waker>,
}

/// A lock whose acquisition is a future: waiting for it parks a native
/// thread (through [`block_on`]) or yields a browser task, never spins and
/// never blocks an event loop. An unlock wakes every waiter and the first
/// to poll takes it (a waiter dropped while waiting can then lose nothing);
/// a caller that needs an order keeps its own (the drivers' streams let a
/// waiting change in before the next read).
pub struct Lock<T> {
    state: Mutex<State>,
    value: UnsafeCell<T>,
}

// The value is reached only through a guard, and one guard exists at a
// time (`State::locked`).
unsafe impl<T: Send> Send for Lock<T> {}
unsafe impl<T: Send> Sync for Lock<T> {}

impl<T> Lock<T> {
    pub fn new(value: T) -> Lock<T> {
        Lock { state: Mutex::new(State { locked: false, waiting: Vec::new() }), value: UnsafeCell::new(value) }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The value, once no one else holds it.
    pub async fn lock(&self) -> LockGuard<'_, T> {
        poll_fn(|cx| {
            let mut s = self.state();
            if !s.locked {
                s.locked = true;
                return Poll::Ready(());
            }
            if !s.waiting.iter().any(|w| w.will_wake(cx.waker())) {
                s.waiting.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await;
        LockGuard { lock: self }
    }

    /// The value now, if no one holds it.
    pub fn try_lock(&self) -> Option<LockGuard<'_, T>> {
        let mut s = self.state();
        if s.locked {
            return None;
        }
        s.locked = true;
        Some(LockGuard { lock: self })
    }
}

/// The lock held; dropping it lets the next waiter in.
pub struct LockGuard<'a, T> {
    lock: &'a Lock<T>,
}

impl<T> Deref for LockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for LockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for LockGuard<'_, T> {
    fn drop(&mut self) {
        let waiting = {
            let mut s = self.lock.state();
            s.locked = false;
            core::mem::take(&mut s.waiting)
        };
        for w in waiting {
            w.wake();
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_lock_is_shared_across_threads() {
        let lock = Arc::new(Lock::new(Vec::new()));
        let held = block_on(lock.lock());
        let mut threads = Vec::new();
        for k in 0..4 {
            let l = Arc::clone(&lock);
            threads.push(std::thread::spawn(move || block_on(l.lock()).push(k)));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert!(lock.try_lock().is_none(), "held");
        drop(held);
        for t in threads {
            t.join().unwrap();
        }
        let mut got = block_on(lock.lock()).clone();
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3]);
        assert!(lock.try_lock().is_some(), "free again");
    }
}
