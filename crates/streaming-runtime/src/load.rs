//! Resource usage of the worker process.

use std::time::Instant;

use parking_lot::Mutex;
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
    fn load(&self) -> Load {
        Load::default()
    }
}

impl<F: Fn() -> Load + Send + Sync + 'static> LoadProbe for F {
    fn load(&self) -> Load {
        self()
    }
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
    fn default() -> Self {
        Self::new()
    }
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
    fn cpu_seconds() -> Option<f64> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        // The command name may contain spaces: skip past its closing parenthesis.
        let rest = &stat[stat.rfind(')')? + 1..];
        let mut fields = rest.split_whitespace();
        let utime: f64 = fields.nth(11)?.parse().ok()?;
        let stime: f64 = fields.next()?.parse().ok()?;
        // USER_HZ is 100 on every Linux configuration in practice.
        Some((utime + stime) / 100.0)
    }

    fn memory() -> Option<(u64, u64)> {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let total_kb: u64 = meminfo
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()?;
        Some((resident_pages * 4096, total_kb * 1024))
    }
}

impl LoadProbe for ProcessLoad {
    fn load(&self) -> Load {
        let now = Instant::now();
        let mut load = Load::default();
        if let Some(cpu) = Self::cpu_seconds() {
            let mut previous = self.previous.lock();
            if let Some((at, before)) = *previous {
                let elapsed = now.duration_since(at).as_secs_f64();
                if elapsed > 0.0 {
                    load.cpu = ((cpu - before) / elapsed).clamp(0.0, 1.0);
                }
            }
            *previous = Some((now, cpu));
        }
        if let Some((resident, total)) = Self::memory() {
            load.memory_bytes = resident;
            if total > 0 {
                load.memory = (resident as f64 / total as f64).clamp(0.0, 1.0);
            }
        }
        load
    }
}
