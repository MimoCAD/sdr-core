//! Receive gain, stage by stage.
//!
//! One gain number hides where the gain is: a HackRF's 48 dB is LNA 40 +
//! VGA 8 with the RF amp off, and stepping that number 5 dB at a time
//! moves one stage 8 dB up and another down, so the noise floor jumps
//! unevenly. Every radio here says
//! what its stages are and where each one sits, and each can be set on
//! its own:
//!
//! - HackRF One: the RF amp (off / +14 dB), the IF LNA (0–40 dB in 8 dB
//!   steps) and the baseband VGA (0–62 dB in 2 dB steps).
//! - RTL-SDR (R820T/R828D): the LNA, the mixer and the VGA, 16 levels
//!   each, their dB from the tuner's own step tables.
//! - USRP B2xx: the AD9364 receive gain, one stage (PGA) of 0–76 dB.
//!
//! A stage is set by LEVEL (an index into its list of settable dB
//! values), so one step is always exactly one hardware step.
//!
//! [`Stage`] names every stage any radio has: the vocabulary for code that
//! does not know which radio it holds (configuration, display, a front-end
//! over several drivers). Each driver has its own stage enum naming only
//! its radio's stages — so asking a radio for a stage it lacks does not
//! compile there — which converts into this one (`From`) and back
//! (`TryFrom`, failing for a stage the radio lacks).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

/// A named gain stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    /// A switched RF amplifier ahead of everything (HackRF: +14 dB).
    Amp,
    /// The low-noise amplifier.
    Lna,
    /// The mixer's gain (R820T).
    Mixer,
    /// The variable-gain amplifier after the mixer / at baseband.
    Vga,
    /// A single programmable-gain chain (the AD9364 as UHD exposes it).
    Pga,
}

impl Stage {
    /// Every stage, in signal order.
    pub const ALL: [Stage; 5] = [Stage::Amp, Stage::Lna, Stage::Mixer, Stage::Vga, Stage::Pga];

    /// The stage's lower-case key (`"lna"`), stable for configuration
    /// files and command lines; [`Stage::from_key`] reads it back.
    pub fn key(self) -> &'static str {
        match self {
            Stage::Amp => "amp",
            Stage::Lna => "lna",
            Stage::Mixer => "mixer",
            Stage::Vga => "vga",
            Stage::Pga => "pga",
        }
    }

    /// A short upper-case display label (`"LNA"`).
    pub fn label(self) -> &'static str {
        match self {
            Stage::Amp => "AMP",
            Stage::Lna => "LNA",
            Stage::Mixer => "MIX",
            Stage::Vga => "VGA",
            Stage::Pga => "PGA",
        }
    }

    pub fn from_key(key: &str) -> Option<Stage> {
        Stage::ALL.into_iter().find(|s| s.key() == key)
    }

    /// Position in [`Stage::ALL`], for per-stage arrays.
    pub fn index(self) -> usize {
        Stage::ALL.iter().position(|&s| s == self).expect("every stage is in ALL")
    }

    /// The error a driver's `TryFrom<Stage>` gives for a stage its radio
    /// does not have.
    pub fn absent(self, backend: &'static str) -> crate::Error {
        crate::Error::Unsupported { backend, what: format!("a {} gain stage (this radio has none)", self.key()) }
    }
}

/// Where one stage sits: its level, how many levels it has, and the dB
/// that level is. `S` is the stage's name: a driver's own stage enum, or
/// [`Stage`] (the default) once converted ([`StageReading::common`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StageReading<S = Stage> {
    pub stage: S,
    pub level: usize,
    pub levels: usize,
    pub db: f64,
}

impl<S: Into<Stage>> StageReading<S> {
    /// The reading under the shared [`Stage`] name.
    pub fn common(self) -> StageReading {
        StageReading { stage: self.stage.into(), level: self.level, levels: self.levels, db: self.db }
    }
}

/// The level whose dB is nearest `db` in a stage's list of levels.
pub fn nearest_level(levels_db: &[f64], db: f64) -> usize {
    levels_db
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| (*a - db).abs().total_cmp(&(*b - db).abs()))
        .map_or(0, |(i, _)| i)
}

/// One human-readable line: `AMP off · LNA 40 · VGA 8 dB (48 dB)`.
pub fn describe(readings: &[StageReading]) -> String {
    let parts: Vec<String> = readings
        .iter()
        .map(|r| match r.stage {
            Stage::Amp => format!("AMP {}", if r.level == 0 { "off" } else { "on" }),
            s => format!("{} {}", s.label(), trim_db(r.db)),
        })
        .collect();
    let total: f64 = readings.iter().map(|r| r.db).sum();
    if readings.len() == 1 {
        format!("{} dB", parts[0])
    } else {
        format!("{} dB ({} dB)", parts.join(" · "), trim_db(total))
    }
}

/// A dB value without a pointless `.0`.
fn trim_db(db: f64) -> String {
    // Nearest whole dB, half away from zero (`f64::round` is std's; gain
    // values are far inside i64).
    let whole = if db >= 0.0 { (db + 0.5) as i64 } else { (db - 0.5) as i64 } as f64;
    if (db - whole).abs() < 0.05 { format!("{:.0}", db) } else { format!("{db:.1}") }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_level_snaps_to_the_closest_step() {
        let lna = [0.0, 8.0, 16.0, 24.0, 32.0, 40.0];
        assert_eq!(nearest_level(&lna, 30.0), 4);
        assert_eq!(nearest_level(&lna, 99.0), 5);
        assert_eq!(nearest_level(&lna, -3.0), 0);
    }

    #[test]
    fn keys_round_trip_and_describe_reads_as_one_line() {
        for s in Stage::ALL {
            assert_eq!(Stage::from_key(s.key()), Some(s));
        }
        let r = [
            StageReading { stage: Stage::Amp, level: 0, levels: 2, db: 0.0 },
            StageReading { stage: Stage::Lna, level: 5, levels: 6, db: 40.0 },
            StageReading { stage: Stage::Vga, level: 4, levels: 32, db: 8.0 },
        ];
        assert_eq!(describe(&r), "AMP off · LNA 40 · VGA 8 dB (48 dB)");
        let one = [StageReading { stage: Stage::Pga, level: 40, levels: 77, db: 40.0 }];
        assert_eq!(describe(&one), "PGA 40 dB");
    }
}
