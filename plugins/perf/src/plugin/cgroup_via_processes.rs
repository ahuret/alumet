//! Workaround: counting a cgroup through its processes, for kernels that miscount multiplexed
//! cgroup events (`cgroup_counting = "via_processes"`).
//!
//! Since Linux 6.2 (bd2756811766, RHEL 9.3), the cgroup events report a wrong `time_running` once
//! multiplexed, so their scaled values are wrong (fixed by e4c7c401c6b9 in Linux 7.3). Events
//! attached to a task are not affected. So each process of the cgroup is counted with `inherit`:
//! what it creates *after* being opened (threads, child processes) is counted by the kernel in its
//! counters. A new process is therefore only opened when no ancestor was already counting when it
//! was born, otherwise it would be counted twice.
//!
//! The processes are found by reading `cgroup.procs` of the cgroup and of its descendants, often
//! ([`Tracker::scan`]). This sees every way into the cgroup (fork, move, `CLONE_INTO_CGROUP`); what
//! an uncounted process does before the next read is lost, which only happens when a new process
//! arrives in the cgroup (e.g. the slurmstepd of a new step, with its first tasks).
//!
//! Everything specific to the workaround is in this module. Once the supported kernels all have the
//! fix, delete it, with the `cgroup_counting` option and its branch in the plugin's `start`.
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alumet::{
    measurement::{MeasurementAccumulator, Timestamp},
    metrics::TypedMetricId,
    pipeline::{Source, elements::error::PollError},
    resources::{Resource, ResourceConsumer},
};
use anyhow::Context;

use super::config::ParsedConfig;
use super::source::{Observable, PerfEventSource, point};
use crate::group::Accuracy;

/// Whether the cgroup events of this kernel can be trusted when multiplexed.
pub(super) fn cgroup_events_reliable() -> bool {
    // The faulty code came with the rewrite of the context handling, which added this function.
    let rewritten = fs::read_to_string("/proc/kallsyms")
        .is_ok_and(|syms| syms.lines().any(|l| l.ends_with(" __pmu_ctx_sched_out")));
    // A kernel based on Linux 7.3 or later has the fix. Distribution kernels with a lower version
    // (RHEL) may have backported the rewrite, but whether they have the fix cannot be told.
    let release = fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    !rewritten || has_fix(release.trim())
}

fn has_fix(release: &str) -> bool {
    let mut numbers = release.split(['.', '-']).map(|n| n.parse::<u32>().ok());
    let (Some(Some(major)), Some(Some(minor))) = (numbers.next(), numbers.next()) else {
        return false;
    };
    // The fix was merged during the 7.3 release candidates.
    (major, minor) > (7, 3) || ((major, minor) == (7, 3) && !release.contains("-rc"))
}

// ---------------------------------------------------------------------------------------------
// procfs and cgroupfs
// ---------------------------------------------------------------------------------------------

/// `(ppid, starttime in clock ticks since boot)` from `/proc/<pid>/stat`.
fn stat_of(pid: u32) -> Option<(u32, u64)> {
    let s = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The comm field may contain spaces and parentheses: parse after the last ')'.
    let f: Vec<&str> = s[s.rfind(')')? + 2..].split(' ').collect();
    // f[0] is field 3 (state): ppid is field 4, starttime field 22.
    Some((f[1].parse().ok()?, f[19].parse().ok()?))
}

/// The processes of a cgroup and of its descendants, `None` once the cgroup is gone.
fn cgroup_procs(dir: &str) -> Option<Vec<u32>> {
    fn walk(dir: &str, pids: &mut Vec<u32>) -> Option<()> {
        let procs = fs::read_to_string(format!("{dir}/cgroup.procs")).ok()?;
        pids.extend(procs.lines().filter_map(|l| l.parse::<u32>().ok()));
        for e in fs::read_dir(dir).ok()?.flatten() {
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                // A sub-cgroup removed meanwhile is not an error.
                let _ = walk(&e.path().to_string_lossy(), pids);
            }
        }
        Some(())
    }
    let mut pids = Vec::new();
    walk(dir, &mut pids)?;
    Some(pids)
}

fn threads_of(pid: u32) -> Vec<u32> {
    fs::read_dir(format!("/proc/{pid}/task"))
        .map(|d| {
            d.flatten()
                .filter_map(|e| e.file_name().to_str()?.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The clock of `starttime` in `/proc/<pid>/stat`.
fn boottime_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn tick_ns() -> u64 {
    1_000_000_000 / unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64
}

// ---------------------------------------------------------------------------------------------
// Which processes to count
// ---------------------------------------------------------------------------------------------

enum Tracked<T> {
    /// Counted with inherit, from every thread that existed when it was opened.
    Opened {
        /// When we started opening it, and when all its threads were counting (CLOCK_BOOTTIME).
        start_boot: u64,
        done_boot: u64,
        /// The counters of each thread opened.
        threads: Vec<T>,
    },
    /// Born after an ancestor was counting: already in the ancestor's counters.
    Covered,
}

/// Opens the counters of a thread and of what it creates afterwards.
type OpenThread<T> = Box<dyn Fn(u32) -> anyhow::Result<T> + Send>;

/// The processes of a cgroup to count, and their counters `T`.
struct Tracker<T> {
    /// Relative to the cgroup root.
    cgroup: String,
    open_thread: OpenThread<T>,
    tracked: HashMap<u32, Tracked<T>>,
}

impl<T> Tracker<T> {
    pub fn new(cgroup: String, open_thread: OpenThread<T>) -> Self {
        Self {
            cgroup,
            open_thread,
            tracked: HashMap::new(),
        }
    }

    /// Opens every thread of `pid`, listing them again until no new one shows up (a thread not
    /// opened yet may create one meanwhile).
    ///
    /// All or nothing: a process missing a thread would look counted, and what that thread creates
    /// would be taken as inherited, so lost. On an error the process is left untracked, and the
    /// next scan tries again.
    fn open_process(&mut self, pid: u32) {
        let start_boot = boottime_ns();
        let mut threads = Vec::new();
        let mut seen = HashSet::new();
        loop {
            let new: Vec<u32> = threads_of(pid).into_iter().filter(|t| seen.insert(*t)).collect();
            if new.is_empty() {
                break;
            }
            for tid in new {
                match (self.open_thread)(tid) {
                    Ok(c) => threads.push(c),
                    // The thread exited meanwhile: nothing to count.
                    Err(e) if is_no_such_task(&e) => (),
                    Err(e) => {
                        log::warn!(
                            "cannot count thread {tid} of process {pid} in {}, will retry: {e:#}",
                            self.cgroup
                        );
                        return;
                    }
                }
            }
        }
        if threads.is_empty() {
            return;
        }
        log::debug!("counting process {pid} ({} threads) of {}", threads.len(), self.cgroup);
        self.tracked.insert(
            pid,
            Tracked::Opened {
                start_boot,
                done_boot: boottime_ns(),
                threads,
            },
        );
    }

    /// Opens the processes of the cgroup that are not counted yet. Returns `false` once the
    /// cgroup is gone.
    pub fn scan(&mut self) -> bool {
        let Some(pids) = cgroup_procs(&format!("/sys/fs/cgroup{}", self.cgroup)) else {
            return false;
        };
        let mut new: Vec<(u32, u32, u64)> = pids
            .into_iter()
            .filter(|p| !self.tracked.contains_key(p))
            .filter_map(|p| stat_of(p).map(|(ppid, start)| (p, ppid, start)))
            .collect();
        // Parents before children.
        new.sort_by_key(|&(_, _, start)| start);
        let tick = tick_ns();
        for (pid, ppid, start) in new {
            // `starttime` is in clock ticks: the process was born in [start, start + 1) ticks.
            let covered = match self.tracked.get(&ppid) {
                Some(Tracked::Covered) => true,
                Some(Tracked::Opened { done_boot, .. }) if start * tick > *done_boot => true,
                Some(Tracked::Opened { start_boot, .. }) if (start + 1) * tick > *start_boot => {
                    // Born while its parent was being opened: counting it twice is better than
                    // losing it.
                    log::debug!("process {pid} of {} may be counted twice", self.cgroup);
                    false
                }
                _ => false,
            };
            if covered {
                self.tracked.insert(pid, Tracked::Covered);
            } else {
                self.open_process(pid);
            }
        }
        true
    }

    /// The counters of every thread opened, including those of processes that have exited: they
    /// keep their final values.
    pub fn counters(&mut self) -> impl Iterator<Item = &mut T> {
        self.tracked.values_mut().flat_map(|t| match t {
            Tracked::Opened { threads, .. } => threads.iter_mut(),
            Tracked::Covered => [].iter_mut(),
        })
    }
}

fn is_no_such_task(e: &anyhow::Error) -> bool {
    e.chain()
        .filter_map(|c| c.downcast_ref::<io::Error>())
        .any(|e| e.raw_os_error() == Some(libc::ESRCH))
}

// ---------------------------------------------------------------------------------------------
// The Alumet source
// ---------------------------------------------------------------------------------------------

/// What identifies a point of the cgroup: metric, resource, pmu.
type PointKey = (TypedMetricId<u64>, Resource, Option<String>);

/// How often the processes of the cgroup are looked for.
const SCAN_INTERVAL: Duration = Duration::from_millis(100);

/// The source of a cgroup counted through its processes: one [`PerfEventSource`] per thread
/// opened, summed and reported as the cgroup.
pub(super) struct CgroupTasksSource {
    consumer: ResourceConsumer,
    tracker: Arc<Mutex<Tracker<PerfEventSource>>>,
}

/// Starts counting the cgroup `cgroup` (relative to the cgroup root) through its processes.
pub(super) fn source(config: &ParsedConfig, cgroup: String) -> anyhow::Result<CgroupTasksSource> {
    log::info!("Starting to observe cgroup {cgroup} through its processes...");
    let (events, metrics, auto_scale) = (
        config.events.clone(),
        config.metrics.clone(),
        config.multiplexing_auto_scale,
    );
    let open_thread: OpenThread<PerfEventSource> = Box::new(move |tid| {
        let observable = Observable::Process {
            pid: tid,
            inherit: true,
        };
        super::build_task_source(observable, &events, &metrics, auto_scale)
    });
    let tracker = Arc::new(Mutex::new(Tracker::new(cgroup.clone(), open_thread)));
    tracker.lock().unwrap().scan();

    // An uncounted process loses what it does until it is found: read cgroup.procs much more
    // often than the poll, in a thread that stops once the source is dropped.
    let weak = Arc::downgrade(&tracker);
    std::thread::Builder::new()
        .name("perf-cgroup-scan".to_owned())
        .spawn(move || {
            loop {
                std::thread::sleep(SCAN_INTERVAL);
                let Some(tracker) = weak.upgrade() else { break };
                tracker.lock().unwrap().scan();
            }
        })
        .context("spawning the cgroup scanner")?;
    Ok(CgroupTasksSource {
        consumer: ResourceConsumer::ControlGroup { path: cgroup.into() },
        tracker,
    })
}

impl Source for CgroupTasksSource {
    fn poll(&mut self, measurements: &mut MeasurementAccumulator, timestamp: Timestamp) -> Result<(), PollError> {
        let mut tracker = self.tracker.lock().unwrap();
        // The sum of every thread, per metric, resource and pmu, with the worst accuracy.
        let mut sums: HashMap<PointKey, (u64, Accuracy)> = HashMap::new();
        for thread in tracker.counters() {
            let read = thread.read_each(|info, metric, value, accuracy| {
                let key = (metric, info.resource.clone(), info.pmu_attr.clone());
                let (sum, acc) = sums.entry(key).or_insert((0, Accuracy::Exact));
                *sum += value;
                *acc = (*acc).max(accuracy);
            });
            if let Err(e) = read {
                log::warn!("could not read the counters of a thread: {e:#}");
            }
        }
        for ((metric, resource, pmu), (sum, accuracy)) in sums {
            measurements.push(point(
                timestamp,
                metric,
                resource,
                self.consumer.clone(),
                pmu,
                sum,
                accuracy,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_releases() {
        assert!(has_fix("7.3.0"));
        assert!(has_fix("7.4.1-arch1"));
        assert!(has_fix("8.0.0"));
        assert!(!has_fix("7.3.0-rc7-vanilla"));
        assert!(!has_fix("6.12.0-55.el10.x86_64"));
        assert!(!has_fix("5.14.0-611.el9_7.x86_64"));
        assert!(!has_fix("garbage"));
    }

    #[test]
    fn own_stat() {
        let (ppid, start) = stat_of(std::process::id()).unwrap();
        assert!(ppid > 0 && start > 0);
    }
}
