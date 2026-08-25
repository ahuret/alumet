//! sysfs helpers for named PMUs: splitting the `pmu/terms` form and reading a PMU's numeric `type`.
//!
//! A named PMU (`cpu_core`, `uncore_imc_0`, `power`, …) is addressed by the numeric id in
//! `/sys/bus/event_source/devices/<pmu>/type`, which goes into `perf_event_attr.type`. Reading a
//! PMU's `cpumask` (to tell system-wide PMUs apart) is added later, alongside the scope detection.

use std::fs;

use anyhow::Context;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_extracts_pmu_and_terms() {
        assert_eq!(split("uncore_imc_0/r0x1/"), Some(("uncore_imc_0", "r0x1")));
        assert_eq!(split("cpu_core/INSTRUCTIONS"), Some(("cpu_core", "INSTRUCTIONS"))); // closing / optional
        assert_eq!(split("cpu/event=0x2e,umask=0x41/"), Some(("cpu", "event=0x2e,umask=0x41")));
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
