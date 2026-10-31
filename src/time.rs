//! A monotonic clock and a sleep that work natively and in a browser.
//!
//! In a browser (`wasm32`) `std::time::Instant::now()` panics and a thread
//! cannot sleep; there the clock is `performance.now()` and [`sleep`] a
//! `setTimeout` awaited. Natively they are `std::time::Instant` and
//! `std::thread::sleep` — the async code of the drivers runs synchronously
//! there (nusb's native operations block), so a native sleep blocks too.

use core::ops::{Add, AddAssign, Sub};
use core::time::Duration;

/// A point on a monotonic clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(imp::Inner);

impl Instant {
    pub fn now() -> Instant {
        Instant(imp::now())
    }

    /// The time since `earlier`, zero if `earlier` is later.
    pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
        imp::since(self.0, earlier.0).unwrap_or(Duration::ZERO)
    }

    /// The time since `earlier`, `None` if `earlier` is later.
    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        imp::since(self.0, earlier.0)
    }

    /// The time since `self`.
    pub fn elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(*self)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, d: Duration) -> Instant {
        Instant(imp::add(self.0, d))
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    /// The time since `rhs`, zero if `rhs` is later.
    fn sub(self, rhs: Instant) -> Duration {
        self.saturating_duration_since(rhs)
    }
}

/// Wait `d`: in a browser without blocking its event loop; natively by
/// blocking the thread.
pub async fn sleep(d: Duration) {
    imp::sleep(d).await
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use core::time::Duration;

    pub type Inner = std::time::Instant;

    pub fn now() -> Inner {
        std::time::Instant::now()
    }

    pub fn since(a: Inner, b: Inner) -> Option<Duration> {
        a.checked_duration_since(b)
    }

    pub fn add(a: Inner, d: Duration) -> Inner {
        a + d
    }

    pub async fn sleep(d: Duration) {
        std::thread::sleep(d);
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use core::time::Duration;

    use js_sys::{Function, Promise, Reflect};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;

    /// Time since the page's (or worker's) time origin.
    pub type Inner = Duration;

    /// `globalThis.<name>`, in a window and in a worker alike.
    fn global(name: &str) -> JsValue {
        Reflect::get(&js_sys::global(), &JsValue::from_str(name)).unwrap_or(JsValue::UNDEFINED)
    }

    pub fn now() -> Inner {
        let perf = global("performance");
        let ms = Reflect::get(&perf, &JsValue::from_str("now"))
            .ok()
            .and_then(|f| f.unchecked_into::<Function>().call0(&perf).ok())
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        Duration::from_secs_f64(ms / 1e3)
    }

    pub fn since(a: Inner, b: Inner) -> Option<Duration> {
        a.checked_sub(b)
    }

    pub fn add(a: Inner, d: Duration) -> Inner {
        a + d
    }

    pub async fn sleep(d: Duration) {
        let ms = d.as_secs_f64() * 1e3;
        let promise = Promise::new(&mut |resolve, _reject| {
            let set_timeout: Function = global("setTimeout").unchecked_into();
            let _ = set_timeout.call2(&JsValue::UNDEFINED, &resolve, &JsValue::from_f64(ms));
        });
        let _ = JsFuture::from(promise).await;
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn the_clock_moves_and_sleeps() {
        let t = Instant::now();
        crate::block_on(sleep(Duration::from_millis(20)));
        let e = t.elapsed();
        assert!(
            e >= Duration::from_millis(20) && e < Duration::from_secs(1),
            "{e:?}"
        );
        assert_eq!(t + e - t, e);
        assert_eq!(t - (t + e), Duration::ZERO);
        assert_eq!(t.checked_duration_since(t + e), None);
    }
}
