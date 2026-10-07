//! The perf [`Source`]: how the configured events are spread over [`EventGroup`]s, and how their
//! values are reported to Alumet.
use std::{
    collections::HashMap,
    fs::File,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use alumet::{
    measurement::{MeasurementAccumulator, MeasurementPoint, Timestamp},
    metrics::TypedMetricId,
    pipeline::{Source, elements::error::PollError},
    resources::{Resource, ResourceConsumer},
};
use anyhow::Context;
use itertools::Itertools;

use crate::event::{ParsedEvent, Scope};
use crate::group::{Accuracy, EventGroup, EventGroupBuilder, Target};
use crate::resource;
use crate::sysfs;
use crate::tasks::{OpenThread, Tracker};

#[derive(Debug)]
pub(super) enum Observable {
    /// Observe a process.
    Process { pid: u32 },
    /// Observe a cgroup. `path` is the canonical cgroup path (relative to the cgroup root).
    Cgroup { path: String, fd: Arc<File> },
    /// Observe a thread and every task it creates afterwards, as part of the cgroup `cgroup`: see
    /// [`CgroupTasksSource`].
    TaskTree { tid: u32, cgroup: String },
    /// Observe the whole machine, for the system-wide PMUs (uncore, `power`, `cstate_*`, …).
    Machine,
}

pub(super) struct PerfEventSource {
    groups: Vec<(EventGroup, GroupInfo)>,
}

/// What a group measures, and the Alumet metric of each of its counters (in `add` order).
struct GroupInfo {
    metrics: Vec<TypedMetricId<u64>>,
    /// Partition key: there is one group per `(cpu_id, key, group)` (a perf group cannot span two
    /// PMUs).
    key: u64,
    cpu_id: Option<u32>,
    /// The group name from the config; `None` for an event with a group of its own.
    group: Option<String>,
    resource: Resource,
    consumer: ResourceConsumer,
    /// The core PMU name, reported as the `pmu` attribute; `None` for events with no specific PMU.
    pmu_attr: Option<String>,
}

impl PerfEventSource {
    /// Reads every group and calls `f` with each counter's value.
    fn read_each(&mut self, mut f: impl FnMut(&GroupInfo, TypedMetricId<u64>, u64, Accuracy)) -> anyhow::Result<()> {
        for (group, info) in &mut self.groups {
            group.read()?;
            for (metric, value) in info.metrics.iter().zip(group.values()) {
                f(info, *metric, *value, group.accuracy());
            }
        }
        Ok(())
    }
}

fn point(
    timestamp: Timestamp,
    metric: TypedMetricId<u64>,
    resource: Resource,
    consumer: ResourceConsumer,
    pmu: Option<String>,
    value: u64,
    accuracy: Accuracy,
) -> MeasurementPoint {
    let point =
        MeasurementPoint::new(timestamp, metric, resource, consumer, value).with_attr("accuracy", accuracy.as_str());
    match pmu {
        Some(pmu) => point.with_attr("pmu", pmu),
        None => point,
    }
}

impl Source for PerfEventSource {
    fn poll(&mut self, measurements: &mut MeasurementAccumulator, timestamp: Timestamp) -> Result<(), PollError> {
        self.read_each(|info, metric, value, accuracy| {
            measurements.push(point(
                timestamp,
                metric,
                info.resource.clone(),
                info.consumer.clone(),
                info.pmu_attr.clone(),
                value,
                accuracy,
            ))
        })?;
        Ok(())
    }
}

/// What identifies a point of a cgroup counted through its processes: metric, resource, pmu.
type PointKey = (TypedMetricId<u64>, Resource, Option<String>);

/// How often the processes of a cgroup counted through its processes are looked for.
const SCAN_INTERVAL: Duration = Duration::from_millis(100);

/// The source of a cgroup counted through its processes, see [`crate::tasks`]: one
/// [`PerfEventSource`] per thread opened, summed.
pub(super) struct CgroupTasksSource {
    tracker: Arc<Mutex<Tracker<PerfEventSource>>>,
}

impl CgroupTasksSource {
    /// `open_thread` opens the counters of a thread of the cgroup ([`Observable::TaskTree`]).
    pub(super) fn start(cgroup: String, open_thread: OpenThread<PerfEventSource>) -> anyhow::Result<Self> {
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
        Ok(Self { tracker })
    }
}

impl Source for CgroupTasksSource {
    fn poll(&mut self, measurements: &mut MeasurementAccumulator, timestamp: Timestamp) -> Result<(), PollError> {
        let mut tracker = self.tracker.lock().unwrap();
        // The sum of every thread, per metric, resource and pmu, with the worst accuracy.
        let mut sums: HashMap<PointKey, (ResourceConsumer, u64, Accuracy)> = HashMap::new();
        for thread in tracker.counters() {
            let read = thread.read_each(|info, metric, value, accuracy| {
                let key = (metric, info.resource.clone(), info.pmu_attr.clone());
                let (_, sum, acc) = sums
                    .entry(key)
                    .or_insert_with(|| (info.consumer.clone(), 0, Accuracy::Exact));
                *sum += value;
                *acc = (*acc).max(accuracy);
            });
            if let Err(e) = read {
                log::warn!("could not read the counters of a thread: {e:#}");
            }
        }
        for ((metric, resource, pmu), (consumer, sum, accuracy)) in sums {
            measurements.push(point(timestamp, metric, resource, consumer, pmu, sum, accuracy));
        }
        Ok(())
    }
}

/// Builder for the perf [`Source`] of a process, a cgroup or the whole machine.
pub(super) struct PerfEventSourceBuilder {
    /// Something to observe.
    observable: Observable,
    /// The groups opened so far, one per `(cpu, pmu, group name)`, and one per event without a
    /// group name.
    groups: Vec<(EventGroupBuilder, GroupInfo)>,
    /// The available CPUs to monitor.
    online_cpus: Vec<u32>,
    /// Activate auto_scaling in case of detected multiplexing
    multiplexing_auto_scale: bool,
}

impl PerfEventSourceBuilder {
    pub(super) fn observe(observable: Observable, multiplexing_auto_scale: bool) -> anyhow::Result<Self> {
        Ok(Self {
            observable,
            groups: Vec::new(),
            online_cpus: sysfs::online_cpus().context("could not detect online CPUs")?,
            multiplexing_auto_scale,
        })
    }

    /// Adds `event` to the perf group named `group`, or to a group of its own if `group` is `None`.
    pub(super) fn add(
        &mut self,
        event: &ParsedEvent,
        group: Option<&str>,
        alumet_metric: TypedMetricId<u64>,
    ) -> anyhow::Result<&mut Self> {
        let key = event.pmu_group_key();
        let group = group.map(str::to_owned);

        match (event.scope(), &self.observable) {
            (Scope::TaskAttached { binding }, Observable::Process { pid }) => {
                let target = Target::Process { pid: *pid };
                let info = GroupInfo {
                    metrics: Vec::new(),
                    key,
                    cpu_id: None,
                    group,
                    resource: resource::for_process(binding.as_ref()),
                    consumer: ResourceConsumer::Process { pid: *pid },
                    pmu_attr: binding.as_ref().map(|b| b.pmu.clone()),
                };
                self.add_to_group(target, info, event, alumet_metric)?;
            }
            (Scope::TaskAttached { binding }, Observable::TaskTree { tid, cgroup }) => {
                let info = GroupInfo {
                    metrics: Vec::new(),
                    key,
                    cpu_id: None,
                    group,
                    resource: resource::for_process(binding.as_ref()),
                    consumer: ResourceConsumer::ControlGroup {
                        path: cgroup.clone().into(),
                    },
                    pmu_attr: binding.as_ref().map(|b| b.pmu.clone()),
                };
                self.add_to_group(Target::TaskTree { tid: *tid }, info, event, alumet_metric)?;
            }
            (Scope::TaskAttached { binding }, Observable::Cgroup { path, fd }) => {
                // Owned copies, so that `self` is no longer borrowed while groups are added.
                let (path, fd) = (path.clone(), fd.clone());
                let cpus = binding
                    .as_ref()
                    .map_or_else(|| self.online_cpus.clone(), |b| b.cpus.clone());
                for cpu in cpus {
                    let target = Target::Cgroup { fd: fd.clone(), cpu };
                    let info = GroupInfo {
                        metrics: Vec::new(),
                        key,
                        cpu_id: Some(cpu),
                        group: group.clone(),
                        resource: resource::for_cgroup_on_cpu(cpu),
                        consumer: ResourceConsumer::ControlGroup {
                            path: path.clone().into(),
                        },
                        pmu_attr: binding.as_ref().map(|b| b.pmu.clone()),
                    };
                    self.add_to_group(target, info, event, alumet_metric)?;
                }
            }
            (Scope::SystemWide { pmu, cpus }, Observable::Machine) => {
                for &cpu in cpus {
                    let info = GroupInfo {
                        metrics: Vec::new(),
                        key,
                        cpu_id: Some(cpu),
                        group: group.clone(),
                        resource: resource::for_system_wide_pmu(pmu, cpu),
                        consumer: ResourceConsumer::LocalMachine,
                        pmu_attr: None,
                    };
                    self.add_to_group(Target::Cpu { cpu }, info, event, alumet_metric)?;
                }
            }
            (Scope::SystemWide { .. }, _) => anyhow::bail!(
                "system-wide event {} can only be counted on the whole machine",
                event.name()
            ),
            (Scope::TaskAttached { .. }, Observable::Machine) => anyhow::bail!(
                "event {} cannot be counted on the whole machine: only system-wide events can",
                event.name()
            ),
        }
        Ok(self)
    }

    /// Adds `event` to the group matching `info`'s `(cpu_id, key, group)`, opening it on `target` if
    /// none exists yet. An event without a group name always opens a new group.
    fn add_to_group(
        &mut self,
        target: Target,
        info: GroupInfo,
        event: &ParsedEvent,
        alumet_metric: TypedMetricId<u64>,
    ) -> anyhow::Result<()> {
        let existing = self.groups.iter().position(|(_, g)| {
            info.group.is_some() && g.group == info.group && g.cpu_id == info.cpu_id && g.key == info.key
        });
        let i = match existing {
            Some(i) => i,
            None => {
                let group = EventGroupBuilder::open(target, self.multiplexing_auto_scale)?;
                self.groups.push((group, info));
                self.groups.len() - 1
            }
        };
        let (group, info) = &mut self.groups[i];
        group.add(event)?;
        info.metrics.push(alumet_metric);
        Ok(())
    }

    pub(super) fn build(self) -> io::Result<PerfEventSource> {
        log::debug!(
            "Built PerfEventSource with groups [{}]",
            self.groups
                .iter()
                .map(|(_, info)| format!(
                    "{{resource: {:?}, consumer: {:?}, cpu: {:?}, group: {:?}, metrics: {:?}}}",
                    info.resource, info.consumer, info.cpu_id, info.group, info.metrics
                ))
                .join(", ")
        );
        let groups = self
            .groups
            .into_iter()
            .map(|(group, info)| Ok((group.build()?, info)))
            .collect::<io::Result<_>>()?;
        Ok(PerfEventSource { groups })
    }
}
