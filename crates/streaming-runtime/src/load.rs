//! Resource usage of the worker process.

use parking_lot::Mutex;
use std::time::Instant;
use streaming_core::control::Load;

/// Measures the load of the process a worker runs in.
///
/// The controller only spreads a graph over more workers when the worker
/// running it reports being saturated, so this is what scaling decisions
/// ultimately rest on. Simulations inject their own probe.
pub trait LoadProbe: Send + Sync + 'static {
    /// Current load.
    fn load(&self) -> Load;
}

/// A probe that always reports an idle process.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoLoad;

impl LoadProbe for NoLoad {
    fn load(&self) -> Load { Load::default() }
}

impl<F: Fn() -> Load + Send + Sync + 'static> LoadProbe for F {
    fn load(&self) -> Load { self() }
}

/// Reads CPU time and resident memory of the current process from `/proc`.
///
/// CPU usage is relative to one core, since a worker is single-threaded.
/// Memory usage is relative to the machine's total memory. On systems without
/// `/proc` the probe reports zeros.
#[derive(Debug)]
pub struct ProcessLoad {
    previous: Mutex<Option<(Instant, f64)>>,
}

impl Default for ProcessLoad {
    fn default() -> Self { Self::new() }
}

impl ProcessLoad {
    /// A new probe; the first reading reports zero CPU.
    #[must_use]
    pub fn new() -> Self {
        Self {
            previous: Mutex::new(None),
        }
    }

    /// CPU seconds consumed by the process so far.
    fn cpu_seconds() -> Option<f64> { parse_cpu_seconds(&std::fs::read_to_string("/proc/self/stat").ok()?) }

    /// Resident and total memory, in bytes.
    fn memory() -> Option<(u64, u64)> {
        parse_memory(
            &std::fs::read_to_string("/proc/self/statm").ok()?,
            &std::fs::read_to_string("/proc/meminfo").ok()?,
        )
    }
}

/// User plus system CPU seconds out of a `/proc/<pid>/stat` line.
fn parse_cpu_seconds(stat: &str) -> Option<f64> {
    // The command name may contain spaces: skip past its closing parenthesis.
    let rest = &stat[stat.rfind(')')? + 1..];
    let mut fields = rest.split_whitespace();
    let utime: f64 = fields.nth(11)?.parse().ok()?;
    let stime: f64 = fields.next()?.parse().ok()?;
    // USER_HZ is 100 on every Linux configuration in practice.
    Some((utime + stime) / 100.0)
}

/// Resident bytes (`/proc/<pid>/statm`, 4 KiB pages) and total bytes
/// (`/proc/meminfo`).
fn parse_memory(statm: &str, meminfo: &str) -> Option<(u64, u64)> {
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let total_kb: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some((resident_pages * 4096, total_kb * 1024))
}

/// `part / whole` within `0..=1`, zero when `whole` is not positive.
fn share(part: f64, whole: f64) -> f64 {
    if whole > 0.0 {
        (part / whole).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

impl LoadProbe for ProcessLoad {
    fn load(&self) -> Load {
        let now = Instant::now();
        let mut load = Load::default();
        if let Some(cpu) = Self::cpu_seconds() {
            let mut previous = self.previous.lock();
            if let Some((at, before)) = *previous {
                load.cpu = share(cpu - before, now.duration_since(at).as_secs_f64());
            }
            *previous = Some((now, cpu));
        }
        if let Some((resident, total)) = Self::memory() {
            load.memory_bytes = resident;
            load.memory = share(resident as f64, total as f64);
        }
        load
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_files_are_parsed() {
        // Fields 14 and 15 are utime and stime, in ticks.
        let stat = "4242 (a (weird) name) S 1 2 3 4 5 6 7 8 9 10 250 150 0 0 20 0";
        assert_eq!(parse_cpu_seconds(stat), Some(4.0));
        assert_eq!(parse_cpu_seconds("4242 no parenthesis"), None);
        assert_eq!(parse_cpu_seconds("1 (x) S 1 2"), None);
        let meminfo = "MemFree:  10 kB\nMemTotal:       2048 kB\nCached: 1 kB\n";
        assert_eq!(
            parse_memory("100 25 3 0 0 0 0", meminfo),
            Some((25 * 4096, 2048 * 1024))
        );
        assert_eq!(parse_memory("100", meminfo), None);
        assert_eq!(parse_memory("100 25", "MemFree: 1 kB"), None);
    }

    #[test]
    fn shares_are_clamped_and_safe() {
        assert!((share(1.0, 4.0) - 0.25).abs() < 1e-12);
        assert!((share(6.0, 4.0) - 1.0).abs() < 1e-12);
        assert!(share(-1.0, 4.0).abs() < 1e-12);
        assert!(share(1.0, 0.0).abs() < 1e-12);
        assert!(share(1.0, -1.0).abs() < 1e-12);
    }

    #[test]
    fn probes_report_what_they_measure() {
        assert_eq!(NoLoad.load(), Load::default());
        let fixed = Load {
            cpu: 0.5,
            ..Load::default()
        };
        assert_eq!((move || fixed).load(), fixed);
        if std::path::Path::new("/proc/self/stat").exists() {
            let probe = ProcessLoad::default();
            let first = probe.load();
            assert!(first.cpu.abs() < 1e-12, "the first reading has no base");
            assert!(first.memory_bytes > 0);
            assert!(first.memory > 0.0 && first.memory <= 1.0);
            // Burn some CPU so that the second reading sees it.
            let start = Instant::now();
            let mut x = 0u64;
            while start.elapsed().as_millis() < 300 {
                x = std::hint::black_box(x.wrapping_mul(31).wrapping_add(7));
            }
            let second = probe.load();
            assert!(second.cpu > 0.0 && second.cpu <= 1.0, "{second:?}");
        }
    }
}
