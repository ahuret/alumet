//! Source of measurements based on Linux perf_events.
use std::{fs::File, io};

use alumet::{
    measurement::{MeasurementAccumulator, MeasurementPoint, Timestamp},
    metrics::TypedMetricId,
    pipeline::{Source, elements::error::PollError},
    resources::{Resource, ResourceConsumer},
};
use anyhow::Context;
use itertools::Itertools;

use crate::cpu;
use crate::multiplexing::{GroupCounters, Snapshot};
use crate::spec::ConfiguredEvent;

#[derive(Debug)]
pub enum Observable {
    /// Observe a process.
    ///
    /// `perf_event_open` can be called with `pid` and `cpu = -1` (any cpu)
    Process { pid: i32 },
    /// Observe a cgroup.
    ///
    /// Unlike processes, cgroups cannot be monitored with `cpu = -1`, a specific cpu id is required
    /// for `perf_event_open` (see https://github.com/torvalds/linux/blob/2c8159388952f530bd260e097293ccc0209240be/kernel/events/core.c#L12487)
    Cgroup { path: String, fd: File },
}

pub struct PerfEventSource {
    event_groups: Vec<EventGroup>,
    multiplexing_auto_scale: bool,
}

struct EventGroup {
    perf_group: perf_event::Group,
    observed_resource: Resource,
    observed_consumer: ResourceConsumer,
    cpu_id: Option<u32>,
    /// The PMU key shared by this group's events (see [`ConfiguredEvent::pmu_group_key`]). A perf
    /// group cannot span two hardware PMUs, so there is one group per `(cpu_id, group_key)`.
    group_key: u64,
    counters: Vec<(perf_event::Counter, TypedMetricId<u64>)>,
    scaling: GroupCounters,
    /// Whether the previous poll found the group starved, so that we only warn on the transition
    /// into starvation and not on every poll while it lasts.
    starved: bool,
}

impl EventGroup {
    /// Reads the group, updates the corrected cumulative value of each of its counters, and logs
    /// anything worth reporting about the multiplexing.
    fn read_and_correct(&mut self, auto_scale: bool) -> Result<(), PollError> {
        use crate::multiplexing::Interval;

        let counts = self.perf_group.read()?;

        // Always available: `new_group_builder` asks for TOTAL_TIME_ENABLED and TOTAL_TIME_RUNNING.
        let (Some(time_enabled), Some(time_running)) = (counts.time_enabled(), counts.time_running()) else {
            return Err(PollError::Fatal(anyhow::anyhow!(
                "perf_events did not report time_enabled/time_running, which are required to detect multiplexing"
            )));
        };
        let now = Snapshot {
            time_enabled: time_enabled.as_nanos(),
            time_running: time_running.as_nanos(),
            values: self.counters.iter().map(|(counter, _)| counts[counter]).collect(),
        };

        let interval = self.scaling.account(now, auto_scale);
        // Only warn on the transition: starvation can last for the whole lifetime of the source.
        let starved = interval == Interval::Starved;
        let just_starved = starved && !self.starved;
        self.starved = starved;
        match interval {
            Interval::Idle | Interval::Exact => (),
            Interval::Multiplexed { running, enabled } => {
                log::debug!(
                    "perf group of {:?} (cpu {:?}) was only on the PMU {:.1}% of the time, its values are {}",
                    self.observed_consumer,
                    self.cpu_id,
                    100.0 * (running as f64) / (enabled as f64),
                    if auto_scale { "extrapolated" } else { "underestimated" },
                );
            }
            Interval::Starved if just_starved => {
                log::warn!(
                    "perf group of {:?} (cpu {:?}) ran but never made it onto the PMU, its counters are stalled. \
                     Possible causes: more events configured than the CPU has hardware counters, another tool \
                     using the PMU system-wide, or a CPU whose PMU does not provide these events (on hybrid CPUs, \
                     generic events are only available on some cores).",
                    self.observed_consumer,
                    self.cpu_id,
                );
            }
            Interval::Starved => (),
        }
        Ok(())
    }
}

impl Source for PerfEventSource {
    fn poll(&mut self, measurements: &mut MeasurementAccumulator, timestamp: Timestamp) -> Result<(), PollError> {
        let auto_scale = self.multiplexing_auto_scale;
        for group in &mut self.event_groups {
            // read all counters in the group and account for the multiplexing
            group.read_and_correct(auto_scale)?;

            // get some metadata about the measurement perimeter
            let resource = &group.observed_resource;
            let consumer = &group.observed_consumer;
            let accuracy = group.scaling.accuracy().as_str();

            // for each counter, push its value (the two vecs are in the same order by construction)
            for ((_, alumet_metric), value) in group.counters.iter().zip(group.scaling.corrected()) {
                measurements.push(
                    MeasurementPoint::new(timestamp, *alumet_metric, resource.clone(), consumer.clone(), *value)
                        .with_attr("accuracy", accuracy),
                )
            }
        }
        Ok(())
    }
}

/// Builder for the perf [`Source`].
pub struct PerfEventSourceBuilder {
    /// Something to observe.
    observable: Observable,
    /// One or multiple groups, all containing the same events.
    groups: Vec<EventGroup>,
    /// The available CPUs to monitor.
    online_cpus: Vec<u32>,
    /// Activate auto_scaling in case of detected multiplexing
    multiplexing_auto_scale: bool,
}

impl PerfEventSourceBuilder {
    pub fn observe(observable: Observable, multiplexing_auto_scale: bool) -> anyhow::Result<Self> {
        Ok(Self {
            observable,
            groups: Vec::new(),
            online_cpus: cpu::online_cpus().context("could not detect online CPUs")?,
            multiplexing_auto_scale,
        })
    }

    pub fn add(&mut self, event: &ConfiguredEvent, alumet_metric: TypedMetricId<u64>) -> anyhow::Result<&mut Self> {
        // Events are partitioned into one group per hardware PMU (a perf group cannot span two): this
        // key selects which group the event joins, or opens.
        let key = event.pmu_group_key();

        // Destructure so the shared borrow of `observable` (for the cgroup fd) and the mutable borrow
        // of `groups` are disjoint.
        let Self {
            observable,
            groups,
            online_cpus,
            ..
        } = self;

        match &*observable {
            Observable::Process { pid } => {
                // Observe the process on any cpu.
                let pid = *pid;
                let consumer = ResourceConsumer::Process {
                    pid: u32::try_from(pid).unwrap(),
                };
                ensure_group_and_add(
                    groups,
                    observable,
                    key,
                    None,
                    Resource::LocalMachine,
                    consumer,
                    event,
                    alumet_metric,
                )?;
            }
            Observable::Cgroup { path, .. } => {
                // Observe the cgroup on each cpu separately (a restriction of perf_event_open).
                let path = path.clone();
                for cpu in online_cpus.iter().copied() {
                    let consumer = ResourceConsumer::ControlGroup {
                        path: path.clone().into(),
                    };
                    ensure_group_and_add(
                        groups,
                        observable,
                        key,
                        Some(cpu),
                        Resource::CpuCore { id: cpu },
                        consumer,
                        event,
                        alumet_metric,
                    )?;
                }
            }
        }
        Ok(self)
    }

    pub fn build(mut self) -> io::Result<PerfEventSource> {
        log::debug!(
            "Built PerfEventSource with groups [{}]",
            self.groups
                .iter()
                .map(|g| format!(
                    "{{resource: {:?}, consumer: {:?}, cpu: {:?}, events: {:?}}}",
                    g.observed_resource, g.observed_consumer, g.cpu_id, g.counters
                ))
                .join(", ")
        );
        for group in &mut self.groups {
            // All the events have been added, the number of counters is now known.
            group.scaling = GroupCounters::new(group.counters.len());
            group.perf_group.enable()?;
        }

        Ok(PerfEventSource {
            event_groups: self.groups,
            multiplexing_auto_scale: self.multiplexing_auto_scale,
        })
    }
}

/// A new group leader builder: a `DUMMY` software event (its value is excluded from `Group::read`,
/// it just anchors the group and carries the read format shared by every counter of the group).
fn new_group_builder<'a>() -> perf_event::Builder<'a> {
    use perf_event::ReadFormat;

    let mut builder = perf_event::Builder::new(perf_event::events::Software::DUMMY);
    builder.read_format(
        ReadFormat::GROUP | ReadFormat::TOTAL_TIME_ENABLED | ReadFormat::TOTAL_TIME_RUNNING | ReadFormat::ID,
    );
    builder
}

/// Point a builder at the observed entity (leader and members must share these settings).
fn point<'o>(builder: &mut perf_event::Builder<'o>, observable: &'o Observable, cpu_id: Option<u32>) {
    match observable {
        Observable::Process { pid } => {
            builder.observe_pid(*pid).any_cpu();
        }
        Observable::Cgroup { fd, .. } => {
            builder
                .observe_cgroup(fd)
                .one_cpu(cpu_id.expect("a cgroup group is always bound to a specific cpu") as usize);
        }
    }
}

/// Add `event` to the group matching `(cpu_id, key)`, creating that group (a fresh `DUMMY` leader)
/// if none exists yet. Every builder — leader and members — is pointed at the same entity.
#[allow(clippy::too_many_arguments)]
fn ensure_group_and_add(
    groups: &mut Vec<EventGroup>,
    observable: &Observable,
    key: u64,
    cpu_id: Option<u32>,
    resource: Resource,
    consumer: ResourceConsumer,
    event: &ConfiguredEvent,
    metric: TypedMetricId<u64>,
) -> anyhow::Result<()> {
    if let Some(group) = groups.iter_mut().find(|g| g.cpu_id == cpu_id && g.group_key == key) {
        let mut event_builder = perf_event::Builder::new(event.encoding());
        point(&mut event_builder, observable, cpu_id);
        event.configure(&mut event_builder);
        let counter = group
            .perf_group
            .add(&event_builder)
            .with_context(|| format!("adding event to group (cpu={cpu_id:?}, pmu_key={key:#x})"))?;
        group.counters.push((counter, metric));
    } else {
        let mut leader = new_group_builder();
        point(&mut leader, observable, cpu_id);
        let mut perf_group = leader
            .build_group()
            .with_context(|| format!("building perf group (cpu={cpu_id:?}, pmu_key={key:#x})"))?;

        let mut event_builder = perf_event::Builder::new(event.encoding());
        point(&mut event_builder, observable, cpu_id);
        event.configure(&mut event_builder);
        let counter = perf_group
            .add(&event_builder)
            .with_context(|| format!("adding first event to group (cpu={cpu_id:?}, pmu_key={key:#x})"))?;

        groups.push(EventGroup {
            perf_group,
            observed_resource: resource,
            observed_consumer: consumer,
            cpu_id,
            group_key: key,
            counters: vec![(counter, metric)],
            scaling: GroupCounters::default(),
            starved: false,
        });
    }
    Ok(())
}

impl PerfEventSource {
    /// Build a machine-wide source for system-wide PMUs (uncore, `power`, `cstate_*`, …).
    ///
    /// Such events are not tied to a process or cgroup: each is opened `pid=-1` on every CPU of its
    /// `cpumask` (`(event, cpu)` gives one group). The measurement is tagged with the resource that
    /// [`resource_of`] derives from the PMU name and the reader CPU's package; the consumer is
    /// `LocalMachine`.
    pub fn build_system_wide(
        multiplexing_auto_scale: bool,
        events: impl IntoIterator<Item = (ConfiguredEvent, TypedMetricId<u64>, String, Vec<u32>)>,
    ) -> anyhow::Result<Self> {
        let mut groups = Vec::new();
        for (event, metric, pmu, cpus) in events {
            let key = event.pmu_group_key();
            for cpu in cpus {
                let cpu_idx = cpu as usize;

                // System-wide PMUs (RAPL/uncore/cstate) reject the `exclude_*` domain bits, and the
                // domain modifiers (`#u`/`#k`/`#h`) are meaningless for them anyway. So we clear the
                // excludes and do *not* apply the event's modifiers, on both leader and member.
                let mut leader = new_group_builder();
                leader
                    .exclude_user(false)
                    .exclude_kernel(false)
                    .exclude_hv(false)
                    .any_pid()
                    .one_cpu(cpu_idx);
                let mut perf_group = leader
                    .build_group()
                    .with_context(|| format!("build_group system-wide on cpu {cpu}"))?;

                let mut event_builder = perf_event::Builder::new(event.encoding());
                event_builder
                    .exclude_user(false)
                    .exclude_kernel(false)
                    .exclude_hv(false)
                    .any_pid()
                    .one_cpu(cpu_idx);
                let counter = perf_group
                    .add(&event_builder)
                    .with_context(|| format!("adding system-wide event on cpu {cpu}"))?;

                groups.push(EventGroup {
                    perf_group,
                    observed_resource: resource_of(&pmu, cpu),
                    observed_consumer: ResourceConsumer::LocalMachine,
                    cpu_id: Some(cpu),
                    group_key: key,
                    counters: vec![(counter, metric)],
                    scaling: GroupCounters::new(1),
                    starved: false,
                });
            }
        }
        for group in &mut groups {
            group.perf_group.enable().context("enabling system-wide group")?;
        }
        Ok(PerfEventSource {
            event_groups: groups,
            multiplexing_auto_scale,
        })
    }
}

/// Map a system-wide PMU to the Alumet [`Resource`] its measurement belongs to.
///
/// The PMU name carries the semantics (these names are kernel-stable), and the reader `cpu`'s
/// package gives the id. A PMU we do not specifically know maps to an explicit `Custom` resource
/// (`kind` = the PMU name, `id` = the reader CPU) rather than guessing a package or core — this is
/// also collision-free, since the reader CPU is unique. If the CPU's package cannot be read, the
/// package-based mappings fall back to that same `Custom` resource.
fn resource_of(pmu: &str, cpu: u32) -> Resource {
    let package = cpu::package_of(cpu).ok();
    match (pmu, package) {
        // per-core PMU: the reader CPU *is* the physical core.
        ("cstate_core", _) => Resource::CpuCore { id: cpu },
        // memory controllers: the RAM of the reader's package.
        (p, Some(pkg)) if p.starts_with("uncore_imc") => Resource::Dram { pkg_id: pkg },
        // package-scoped PMUs we know: RAPL and package C-states.
        (p, Some(pkg)) if p == "power" || p.starts_with("cstate_pkg") => Resource::CpuPackage { id: pkg },
        // anything else (other uncore boxes, unknown PMUs): explicit rather than guessed.
        _ => Resource::Custom {
            kind: pmu.to_owned().into(),
            id: cpu.to_string().into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_of_maps_core_and_unknown_pmus() {
        // cstate_core: the reader CPU is the physical core (no topology lookup needed).
        assert_eq!(resource_of("cstate_core", 2), Resource::CpuCore { id: 2 });
        // An unknown PMU: explicit Custom, collision-free (id = the reader CPU).
        assert_eq!(
            resource_of("uncore_cbox_0", 5),
            Resource::Custom {
                kind: "uncore_cbox_0".to_owned().into(),
                id: "5".to_owned().into(),
            }
        );
    }

    #[test]
    fn resource_of_maps_package_pmus_when_topology_is_available() {
        // power -> package, uncore_imc -> that package's DRAM. Needs sysfs topology; skip if absent.
        let Ok(pkg) = cpu::package_of(0) else {
            eprintln!("skipping resource_of_maps_package_pmus_when_topology_is_available: no topology");
            return;
        };
        assert_eq!(resource_of("power", 0), Resource::CpuPackage { id: pkg });
        assert_eq!(resource_of("uncore_imc_0", 0), Resource::Dram { pkg_id: pkg });
    }
}
