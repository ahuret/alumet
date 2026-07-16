//! Support for hardcoded perf events, given by their raw `perf_event_attr` fields.
//!
//! This is an escape hatch for advanced users who know the exact encoding of an event on a
//! specific CPU: they provide the raw `config` code (and optionally `config1`/`config2`) plus
//! the `type`, instead of relying on the kernel's generic tables or on libpfm.
//!
//! The `type` can be:
//! - given explicitly (`type = 4` for `PERF_TYPE_RAW`, the default), or
//! - resolved at runtime from a PMU device via `pmu = "<name>"`, which reads
//!   `/sys/bus/event_source/devices/<name>/type`. This covers the case where the type is a
//!   dynamic number assigned by the kernel (uncore, RAPL, …) and thus not known in advance.
//!
//! NOTE: uncore/shared PMUs also need a system-wide (`pid = -1`) opening mode and per-domain
//! deduplication (`cpumask`), which this plugin does not implement yet. A raw event targeting
//! such a PMU will be encoded correctly but opened through the per-process/cgroup model, which
//! is not correct for uncore counters. That is a separate, future piece of work.

use anyhow::{Context, bail};
use perf_event::events::Event;
use perf_event_open_sys::bindings::perf_event_attr;
use serde::{Deserialize, Serialize};

use crate::events::NamedPerfEvent;

/// The kernel's `PERF_TYPE_RAW`, used as the default `type` for hardcoded events.
const PERF_TYPE_RAW: u32 = 4;

/// A hardcoded event, defined by explicit `perf_event_attr` fields.
///
/// Implements [`Event`] by copying those fields into the `perf_event_attr`, so it plugs into
/// the generic perf source builder like any other event.
#[derive(Debug, Clone, Copy)]
pub struct RawEvent {
    type_: u32,
    config: u64,
    config1: u64,
    config2: u64,
}

impl Event for RawEvent {
    fn update_attrs(self, attr: &mut perf_event_attr) {
        attr.type_ = self.type_;
        attr.config = self.config;
        attr.config1 = self.config1;
        attr.config2 = self.config2;
    }
}

/// Configuration entry for a hardcoded event (one item of the `raw_events` list).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawEventDef {
    /// Metric name suffix: the metric is `perf_raw_{name}`.
    pub name: String,
    /// Event code (`perf_event_attr.config`), as hex (`0x…`) or decimal.
    pub config: String,
    /// Optional `perf_event_attr.config1` (hex or decimal), defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config1: Option<String>,
    /// Optional `perf_event_attr.config2` (hex or decimal), defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config2: Option<String>,
    /// Explicit `perf_event_attr.type`. Mutually exclusive with `pmu`. Defaults to
    /// `PERF_TYPE_RAW` (4) when neither is set.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_: Option<u32>,
    /// PMU device name to resolve the `type` from
    /// `/sys/bus/event_source/devices/<pmu>/type`. Mutually exclusive with `type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pmu: Option<String>,
}

/// Returns a hardcoded perf event from its configuration entry.
///
/// The metric name is derived from `def.name` via [`metric_suffix`].
pub fn parse_raw(def: &RawEventDef) -> anyhow::Result<NamedPerfEvent<RawEvent>> {
    let type_ = resolve_type(def).with_context(|| format!("invalid raw event '{}'", def.name))?;
    let config = parse_u64(&def.config).with_context(|| format!("raw event '{}': invalid config", def.name))?;
    let config1 = parse_opt(&def.config1).with_context(|| format!("raw event '{}': invalid config1", def.name))?;
    let config2 = parse_opt(&def.config2).with_context(|| format!("raw event '{}': invalid config2", def.name))?;

    Ok(NamedPerfEvent {
        name: metric_suffix(&def.name),
        description: format!("hardcoded event (type={type_}, config={config:#x})"),
        event: RawEvent {
            type_,
            config,
            config1,
            config2,
        },
    })
}

/// Determine the `perf_event_attr.type`: explicit `type`, else resolved from `pmu`, else RAW.
fn resolve_type(def: &RawEventDef) -> anyhow::Result<u32> {
    match (def.type_, &def.pmu) {
        (Some(_), Some(_)) => bail!("set either `type` or `pmu`, not both"),
        (Some(t), None) => Ok(t),
        (None, Some(pmu)) => read_pmu_type(pmu),
        (None, None) => Ok(PERF_TYPE_RAW),
    }
}

/// Read the dynamic `type` of a PMU from sysfs.
fn read_pmu_type(pmu: &str) -> anyhow::Result<u32> {
    let path = format!("/sys/bus/event_source/devices/{pmu}/type");
    let content = std::fs::read_to_string(&path).with_context(|| format!("cannot read PMU type from {path}"))?;
    content
        .trim()
        .parse::<u32>()
        .with_context(|| format!("invalid PMU type in {path}: '{}'", content.trim()))
}

/// Parse an unsigned 64-bit value, accepting hex (`0x…`) or decimal.
fn parse_u64(s: &str) -> anyhow::Result<u64> {
    let t = s.trim();
    match t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).with_context(|| format!("invalid hex number '{s}'")),
        None => t.parse::<u64>().with_context(|| format!("invalid number '{s}'")),
    }
}

fn parse_opt(s: &Option<String>) -> anyhow::Result<u64> {
    match s {
        Some(s) => parse_u64(s),
        None => Ok(0),
    }
}

/// Turn a user-provided event name into a metric-name suffix, e.g. `my event:1` ->
/// `my_event_1`. Case is preserved; non-alphanumeric characters become `_`.
fn metric_suffix(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_and_decimal() {
        assert_eq!(parse_u64("0x00c0").unwrap(), 0xc0);
        assert_eq!(parse_u64("0XFF").unwrap(), 0xff);
        assert_eq!(parse_u64("192").unwrap(), 192);
        assert!(parse_u64("nope").is_err());
    }

    #[test]
    fn default_type_is_raw() {
        let def = RawEventDef {
            name: "e".to_owned(),
            config: "0xc0".to_owned(),
            config1: None,
            config2: None,
            type_: None,
            pmu: None,
        };
        let parsed = parse_raw(&def).unwrap();
        assert_eq!(parsed.event.type_, PERF_TYPE_RAW);
        assert_eq!(parsed.event.config, 0xc0);
    }

    #[test]
    fn explicit_type_and_config1() {
        let def = RawEventDef {
            name: "raw:evt".to_owned(),
            config: "0x01".to_owned(),
            config1: Some("0x10".to_owned()),
            config2: None,
            type_: Some(10),
            pmu: None,
        };
        let parsed = parse_raw(&def).unwrap();
        assert_eq!(parsed.name, "raw_evt");
        assert_eq!(parsed.event.type_, 10);
        assert_eq!(parsed.event.config1, 0x10);
    }

    #[test]
    fn type_and_pmu_conflict() {
        let def = RawEventDef {
            name: "e".to_owned(),
            config: "0x1".to_owned(),
            config1: None,
            config2: None,
            type_: Some(4),
            pmu: Some("amd_l3".to_owned()),
        };
        assert!(parse_raw(&def).is_err());
    }
}
