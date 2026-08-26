//! sysfs helpers for named PMUs: splitting the `pmu/terms` form, reading a PMU's numeric `type`, and
//! its `cpumask`.
//!
//! A named PMU (`cpu_core`, `uncore_imc_0`, `power`, …) is addressed by the numeric id in
//! `/sys/bus/event_source/devices/<pmu>/type`, which goes into `perf_event_attr.type`. Its `cpumask`
//! (when present) tells the system-wide PMUs apart from the task-attachable core ones.

use std::{fs, io};

use anyhow::Context;

use crate::cpu::parse_cpu_list;

/// Split a `pmu/terms` string into its PMU name and inner term list.
///
/// The closing `/` is optional, so both `uncore_imc_0/r0x1/` (perf's canonical form) and
/// `uncore_imc_0/r0x1` are accepted. Returns `None` if `name` is not of that shape: no `/` at all,
/// an empty PMU name, empty terms, or a nested `/`. The `terms` are returned unparsed (each encoder
/// interprets them).
pub fn split(name: &str) -> Option<(&str, &str)> {
    let (pmu, terms) = name.split_once('/')?;
    let terms = terms.strip_suffix('/').unwrap_or(terms); // closing `/` is optional
    if pmu.is_empty() || terms.is_empty() || terms.contains('/') {
        return None;
    }
    Some((pmu, terms))
}

/// Read a PMU's numeric perf `type` from sysfs (`.../devices/<pmu>/type`).
pub fn read_type(pmu: &str) -> anyhow::Result<u32> {
    let path = format!("/sys/bus/event_source/devices/{pmu}/type");
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("cannot read {path}; is '{pmu}' a valid PMU? (see /sys/bus/event_source/devices)"))?;
    raw.trim()
        .parse::<u32>()
        .with_context(|| format!("invalid PMU type in {path}: {:?}", raw.trim()))
}

/// Read a PMU's `cpumask` — the CPUs designated to read a system-wide PMU.
///
/// Returns `Ok(None)` when the PMU has no `cpumask` file: that PMU is task-attachable (a core PMU,
/// which exposes `cpus` instead), so its events follow the observed process/cgroup. Returns
/// `Ok(Some(cpus))` when the PMU is system-wide (uncore, `power`, `cstate_*`, …): its events are not
/// tied to a task and must be opened once on each of these CPUs.
pub fn read_cpumask(pmu: &str) -> anyhow::Result<Option<Vec<u32>>> {
    let path = format!("/sys/bus/event_source/devices/{pmu}/cpumask");
    match fs::read_to_string(&path) {
        Ok(list) => Ok(Some(
            parse_cpu_list(&list).with_context(|| format!("invalid cpumask in {path}"))?,
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("cannot read {path}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_extracts_pmu_and_terms() {
        assert_eq!(split("uncore_imc_0/r0x1/"), Some(("uncore_imc_0", "r0x1")));
        assert_eq!(split("cpu_core/INSTRUCTIONS"), Some(("cpu_core", "INSTRUCTIONS"))); // closing / optional
        assert_eq!(
            split("cpu/event=0x2e,umask=0x41/"),
            Some(("cpu", "event=0x2e,umask=0x41"))
        );
    }

    #[test]
    fn split_rejects_other_shapes() {
        assert_eq!(split("r3c"), None); // no `/` at all
        assert_eq!(split("cpu_core/"), None); // empty terms
        assert_eq!(split("/r3c/"), None); // empty PMU
        assert_eq!(split("a/b/c/"), None); // nested slash
    }

    #[test]
    fn read_type_matches_sysfs_for_an_available_pmu() {
        // Reads a real PMU from sysfs; skips cleanly where /sys is unavailable (e.g. a sandbox).
        let Some((pmu, expected)) = first_available_pmu() else {
            eprintln!("skipping read_type_matches_sysfs_for_an_available_pmu: no PMU found in sysfs");
            return;
        };
        assert_eq!(read_type(&pmu).unwrap(), expected);
    }

    #[test]
    fn read_type_rejects_unknown_pmu() {
        assert!(read_type("definitely_not_a_pmu_xyz").is_err());
    }

    #[test]
    fn cpumask_absent_reads_as_none() {
        // A PMU with no `cpumask` file (here: a non-existent one) is treated as task-attachable, so
        // the classification is `None` rather than an error.
        assert_eq!(read_cpumask("definitely_not_a_pmu_xyz").unwrap(), None);
    }

    #[test]
    fn cpumask_present_reads_as_some() {
        // Finds a real system-wide PMU (uncore, power, cstate…) and checks its cpumask is non-empty.
        // Skips cleanly if /sys has none (e.g. a sandbox, or a machine with no such PMU).
        let Some(pmu) = first_pmu_with_cpumask() else {
            eprintln!("skipping cpumask_present_reads_as_some: no system-wide PMU found in sysfs");
            return;
        };
        let cpus = read_cpumask(&pmu).unwrap().expect("cpumask file exists");
        assert!(!cpus.is_empty(), "cpumask of {pmu} should list at least one CPU");
    }

    /// Find any PMU exposing a `cpumask` in sysfs (a system-wide PMU), returning its name.
    fn first_pmu_with_cpumask() -> Option<String> {
        let entries = std::fs::read_dir("/sys/bus/event_source/devices").ok()?;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if matches!(read_cpumask(&name), Ok(Some(_))) {
                return Some(name);
            }
        }
        None
    }

    /// Find any PMU exposing a numeric `type` in sysfs, returning its name and type.
    fn first_available_pmu() -> Option<(String, u32)> {
        let entries = std::fs::read_dir("/sys/bus/event_source/devices").ok()?;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Ok(t) = read_type(&name) {
                return Some((name, t));
            }
        }
        None
    }
}
