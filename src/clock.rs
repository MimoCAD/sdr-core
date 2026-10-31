//! Sample-clock discipline: the time of every sample over days and months.
//!
//! A receiver that counts samples keeps time with the radio's crystal:
//! sample `n` is `n / rate` after the start. The crystal is off by tens of
//! ppm and wanders with temperature, so over a long run that clock walks
//! away from real time — 25 ppm is 2 s a day, 6 minutes in 180 days. The
//! host's clock, disciplined by NTP, is right to ~0.1 ppm in the long run
//! and ~1 ms at any instant, but only in the long run: each reading of it
//! against the stream (a transfer's completion) is late by a jittery
//! USB and scheduling delay.
//!
//! [`SampleClock`] takes both: the sample count is the clock between
//! updates (smooth, no jitter), steered toward the host clock by a slow
//! loop (phase and rate, the way NTP disciplines a system clock), so
//! the time of a sample stays within about a millisecond of the host's for
//! as long as the stream runs, and the loop's rate is the crystal's error
//! measured ([`SampleClock::ppm`]).
//!
//! Each completion is offered with [`SampleClock::observe`]: the samples
//! received so far and the host's *monotonic* time then (seconds; e.g. an
//! `Instant` against a fixed origin — NTP slews it but never steps it).
//! Per window the least-delayed observation is the measurement (delay only
//! ever adds). An offset beyond `step_threshold` that persists is not
//! smoothed in: the model is re-anchored and [`ClockEvent::Step`] reported
//! (samples lost, or the device stalled). A break the stream makes on
//! purpose is announced with [`SampleClock::resume`].
//!
//! Losses smaller than the host's timing noise (a few hundred µs) are not
//! visible here; they are the stream's own accounting to report.

use core::fmt;

/// The loop's settings.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClockConfig {
    /// One measurement per window: the least-delayed observation in it.
    pub window_s: f64,
    /// The loop's time constant at the start (fast acquisition) and the
    /// longest it grows to while the offset stays small.
    pub tau_min_s: f64,
    pub tau_max_s: f64,
    /// An offset beyond this, `step_windows` windows running, is a step.
    pub step_threshold_s: f64,
    pub step_windows: u32,
}

impl Default for ClockConfig {
    fn default() -> Self {
        ClockConfig {
            window_s: 1.0,
            tau_min_s: 8.0,
            tau_max_s: 256.0,
            step_threshold_s: 0.005,
            step_windows: 3,
        }
    }
}

/// What an observation changed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ClockEvent {
    /// The first observation: the model anchored at it.
    Anchored,
    /// The samples ran `offset_s` away from the host clock and stayed
    /// there: the model re-anchored at sample `at_sample`. Positive: the
    /// host clock ran ahead (samples missing, ≈ `offset_s × rate`).
    Step { at_sample: u64, offset_s: f64 },
}

/// The time of every sample, disciplined to the host's clock.
#[derive(Clone, Debug)]
pub struct SampleClock {
    cfg: ClockConfig,
    nominal_rate: f64,
    /// The model: t(n) = t0 + (n − n0) × period, in host seconds.
    t0: f64,
    n0: u64,
    period: f64,
    anchored: bool,
    win_start: f64,
    win_min: f64,
    tau: f64,
    /// Windows since the time constant last changed, all small.
    calm: u32,
    /// Consecutive windows beyond the step threshold, and their offset.
    over: u32,
    over_offset: f64,
    /// The last window's measured offset.
    last_offset: f64,
    steps: u64,
}

impl SampleClock {
    pub fn new(nominal_rate: f64) -> SampleClock {
        SampleClock::with_config(nominal_rate, ClockConfig::default())
    }

    pub fn with_config(nominal_rate: f64, cfg: ClockConfig) -> SampleClock {
        SampleClock {
            cfg,
            nominal_rate,
            t0: 0.0,
            n0: 0,
            period: 1.0 / nominal_rate,
            anchored: false,
            win_start: 0.0,
            win_min: f64::INFINITY,
            tau: cfg.tau_min_s,
            calm: 0,
            over: 0,
            over_offset: 0.0,
            last_offset: 0.0,
            steps: 0,
        }
    }

    /// The host time of sample `n` (host monotonic seconds).
    pub fn time_of(&self, n: u64) -> f64 {
        self.t0 + (n as f64 - self.n0 as f64) * self.period
    }

    /// The sample rate the loop has measured.
    pub fn rate(&self) -> f64 {
        1.0 / self.period
    }

    /// The crystal's error against the host clock, in ppm (positive: the
    /// device's clock runs fast).
    pub fn ppm(&self) -> f64 {
        (self.rate() / self.nominal_rate - 1.0) * 1e6
    }

    /// The last window's offset (host − model, seconds): how far the
    /// model sat from the host clock before the loop's correction.
    pub fn offset(&self) -> f64 {
        self.last_offset
    }

    /// The loop's time constant now.
    pub fn tau(&self) -> f64 {
        self.tau
    }

    pub fn steps(&self) -> u64 {
        self.steps
    }

    pub fn is_anchored(&self) -> bool {
        self.anchored
    }

    /// Re-base the model at `(n, t)` keeping the rate: the prediction is
    /// unchanged, only the origin moves (so a rate change applies from
    /// here on, not retroactively).
    fn rebase(&mut self, n: u64) {
        self.t0 = self.time_of(n);
        self.n0 = n;
    }

    /// One completion: `samples` received so far, at host monotonic time
    /// `t` (seconds).
    pub fn observe(&mut self, samples: u64, t: f64) -> Option<ClockEvent> {
        if !self.anchored {
            self.t0 = t;
            self.n0 = samples;
            self.win_start = t;
            self.win_min = f64::INFINITY;
            self.anchored = true;
            return Some(ClockEvent::Anchored);
        }
        let e = t - self.time_of(samples);
        if e < self.win_min {
            self.win_min = e;
        }
        if t - self.win_start < self.cfg.window_s {
            return None;
        }
        let x = self.win_min;
        self.win_start = t;
        self.win_min = f64::INFINITY;
        self.last_offset = x;

        if x.abs() > self.cfg.step_threshold_s {
            self.over += 1;
            self.over_offset = x;
            if self.over >= self.cfg.step_windows {
                // Re-anchor at the measurement; the rate is kept, the loop
                // restarts fast to settle the new origin.
                self.rebase(samples);
                self.t0 += self.over_offset;
                self.over = 0;
                self.tau = self.cfg.tau_min_s;
                self.calm = 0;
                self.steps += 1;
                return Some(ClockEvent::Step {
                    at_sample: samples,
                    offset_s: x,
                });
            }
            return None;
        }
        self.over = 0;

        // The loop (per window W, time constant τ): phase moved by W/τ of
        // the offset, the rate by W/(2τ²) — a second-order loop damped at
        // ~0.7. A positive offset (host later than the model) means the
        // samples come slower than modelled: the period grows.
        let w = self.cfg.window_s;
        self.rebase(samples);
        self.t0 += x * (w / self.tau);
        self.period *= 1.0 + x * w / (2.0 * self.tau * self.tau);

        // The time constant doubles after 4τ of calm windows, to the
        // longest: fast to acquire, then quiet against the host's jitter.
        if x.abs() < self.cfg.step_threshold_s / 5.0 {
            self.calm += 1;
            if f64::from(self.calm) * w >= 4.0 * self.tau && self.tau < self.cfg.tau_max_s {
                self.tau = (self.tau * 2.0).min(self.cfg.tau_max_s);
                self.calm = 0;
            }
        } else {
            self.calm = 0;
        }
        None
    }

    /// The stream resumes after a break it made on purpose (a pause, a
    /// restart): sample `samples` is at host time `t`. The rate and the
    /// loop's state are kept; the origin moves.
    pub fn resume(&mut self, samples: u64, t: f64) {
        if !self.anchored {
            return;
        }
        self.t0 = t;
        self.n0 = samples;
        self.win_start = t;
        self.win_min = f64::INFINITY;
        self.over = 0;
    }
}

impl fmt::Display for SampleClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:+.3} ppm, offset {:+.3} ms, τ {:.0} s, {} steps",
            self.ppm(),
            self.last_offset * 1e3,
            self.tau,
            self.steps
        )
    }
}

// The simulations print and take `sin`, both std's.
#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    /// A small deterministic generator for the delays (no dependency).
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    /// A simulated run: the device's true rate `nominal × (1 + ppm(t)/1e6)`,
    /// a completion every `every_s` seconds observed `delay` late (a
    /// floor plus jitter, now and then a long stall); returns the worst
    /// |time_of − true time| after `settle_s`, and the final ppm.
    fn run(
        nominal: f64,
        ppm: impl Fn(f64) -> f64,
        days: f64,
        every_s: f64,
        cfg: ClockConfig,
        mut on: impl FnMut(&mut SampleClock, f64, u64),
    ) -> (f64, f64, SampleClock) {
        let mut c = SampleClock::with_config(nominal, cfg);
        let mut rng = Lcg(42);
        let (mut t, mut n) = (0.0f64, 0f64);
        let mut worst = 0.0f64;
        let end = days * 86_400.0;
        let settle = 3_600.0;
        while t < end {
            n += nominal * (1.0 + ppm(t) * 1e-6) * every_s;
            t += every_s;
            let delay = 0.000_2
                + 0.001 * rng.next()
                + if rng.next() < 0.01 {
                    0.01 * rng.next()
                } else {
                    0.0
                };
            c.observe(n as u64, t + delay);
            on(&mut c, t, n as u64);
            if t > settle {
                worst = worst.max((c.time_of(n as u64) - t).abs());
            }
        }
        let p = c.ppm();
        (worst, p, c)
    }

    #[test]
    fn a_constant_offset_is_measured_and_held() {
        let (worst, ppm, c) = run(
            2.4e6,
            |_| 40.0,
            0.25,
            0.005,
            ClockConfig::default(),
            |_, _, _| {},
        );
        eprintln!(
            "constant 40 ppm, 6 h: worst {:.3} ms, ppm {ppm:.4}",
            worst * 1e3
        );
        assert!(worst < 0.0015, "worst {worst}");
        assert!((ppm - 40.0).abs() < 0.05, "ppm {ppm}");
        assert_eq!(c.steps(), 0);
    }

    /// 180 days, the crystal wandering ±8 ppm a day around 25 ppm (a
    /// temperature cycle) and a slow ageing: the time of a sample stays
    /// within 1 ms of the truth throughout (measured 0.56 ms with the
    /// default 256 s longest time constant; 1.8 ms at 1,024 s, which lags
    /// a daily temperature cycle).
    #[test]
    fn half_a_year_of_drift_stays_bounded() {
        let ppm = |t: f64| {
            25.0 + 8.0 * (2.0 * std::f64::consts::PI * t / 86_400.0).sin() + t / 86_400.0 * 0.02
        };
        let cfg = ClockConfig {
            window_s: 4.0,
            ..ClockConfig::default()
        };
        let (worst, _, c) = run(2.4e6, ppm, 180.0, 1.0, cfg, |_, _, _| {});
        eprintln!(
            "180 days, 25 ± 8 ppm daily + ageing: worst {:.3} ms, final {c}",
            worst * 1e3
        );
        assert!(worst < 0.001, "worst {worst}");
        assert_eq!(c.steps(), 0, "{c}");
    }

    /// A 20 ms loss (48,000 samples at 2.4 Msps) after an hour: reported as
    /// a step of ~+20 ms, and the time of later samples is right again.
    #[test]
    fn a_large_loss_is_a_step() {
        let mut lost = false;
        let mut seen = None;
        let mut c = SampleClock::new(2.4e6);
        let mut rng = Lcg(7);
        let (mut t, mut n) = (0.0f64, 0u64);
        while t < 7_200.0 {
            t += 0.005;
            n += 12_000;
            if !lost && t > 3_600.0 {
                n -= 48_000;
                lost = true;
            }
            if let Some(ClockEvent::Step { offset_s, .. }) =
                c.observe(n, t + 0.0002 + 0.001 * rng.next())
            {
                seen = Some(offset_s);
            }
        }
        let off = seen.expect("a step");
        assert!((off - 0.020).abs() < 0.002, "offset {off}");
        assert!((c.time_of(n) - t).abs() < 0.002);
    }

    /// A deliberate break: the origin moves, the measured rate stays.
    #[test]
    fn resume_keeps_the_rate() {
        let (_, ppm, mut c) = run(
            1.2e6,
            |_| -30.0,
            0.1,
            0.01,
            ClockConfig::default(),
            |_, _, _| {},
        );
        let before = c.ppm();
        c.resume(10_000_000_000, 1e6);
        assert_eq!(c.ppm(), before);
        assert!((ppm + 30.0).abs() < 0.1);
        assert!((c.time_of(10_000_000_000) - 1e6).abs() < 1e-9);
    }
}
