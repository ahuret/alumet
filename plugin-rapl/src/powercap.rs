// See https://www.kernel.org/doc/html/latest/power/powercap/powercap.html
// for an explanation of the Power Capping framework.

use std::{
    fmt::Display,
    fs::{self, File},
    io::{Read, Seek},
    path::{Path, PathBuf},
};

use alumet::plugin::util::{CounterDiff, CounterDiffUpdate};
use alumet::resources::Resource;
use alumet::{
    measurement::{AttributeValue, MeasurementAccumulator, MeasurementPoint, Timestamp},
    resources::ResourceConsumer,
};
use alumet::{metrics::TypedMetricId, pipeline::elements::error::PollError};
use anyhow::{anyhow, Context};

use super::domains::RaplDomainType;

pub const POWERCAP_RAPL_PATH: &str = "/sys/devices/virtual/powercap/intel-rapl";
const POWER_ZONE_PREFIX: &str = "intel-rapl";
const POWERCAP_ENERGY_UNIT: f64 = 0.000_001; // 1 microJoules

const PERMISSION_ADVICE: &str = "Try to adjust file permissions.";

pub struct PowerZoneFactory;

/// A power zone.
#[derive(Debug, Clone, PartialEq)]
pub struct PowerZone {
    /// The name of the zone, as returned by powercap, for instance `package-0` or `core`.
    pub name: String,

    /// The RAPL domain type, as an enum
    pub domain: RaplDomainType,

    /// The path of the zone in sysfs, for instance
    /// `/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0`.
    ///
    /// Note that in the above path, `intel-rapl` is the "control type"
    /// and "intel-rapl:0" is the power zone.
    /// On my machine, that zone is named `package-0`.
    pub path: PathBuf,

    /// The id of the socket that "contains" this zone, if applicable (psys has no socket)
    pub socket_id: Option<u32>,
}

/// manages power zone counter collection
struct OpenedPowerZone {
    file: File,
    domain: RaplDomainType,
    /// The corresponding ResourceId
    resource: Resource,
    /// Overflow-correcting counter, to compute the energy consumption difference.
    counter: CounterDiff,
}

/// Powercap probe collects Alumet metrics related to power zones
pub struct PowercapProbe {
    metric: TypedMetricId<f64>,

    /// Ready-to-use powercap zones with additional metadata
    zones: Vec<OpenedPowerZone>,
}

/// Retrieves all Powercap power zones from /sys/devices/virtual/powercap/intel-rapl base path.
pub fn all_power_zones() -> anyhow::Result<Vec<PowerZone>> {
    all_power_zones_from_path(Path::new(POWERCAP_RAPL_PATH))
}

fn all_power_zones_from_path(path: &Path) -> anyhow::Result<Vec<PowerZone>> {
    let mut zones = Vec::new();

    fn explore_path_rec(path: &Path, zones: &mut Vec<PowerZone>) -> anyhow::Result<()> {
        for e in fs::read_dir(path)? {
            let entry = e?;
            let path = entry.path();
            if let Some(zone) = PowerZoneFactory::from_path(&path)? {
                zones.push(zone);
                let _ = explore_path_rec(&path, zones);
            }
        }
        Ok(())
    }

    explore_path_rec(path, &mut zones)?;

    zones.sort_by_key(|z: &PowerZone| z.path.to_string_lossy().to_string());
    Ok(zones)
}

impl PowerZoneFactory {
    /// creates a new PowerZone from a zone base path. In case the path is not identified as a zone path, None will be returned.
    /// (eg: /sys/devices/virtual/powercap/intel-rapl/intel-rapl:0)
    fn from_path(path: &Path) -> anyhow::Result<Option<PowerZone>> {
        Ok(match Self::is_zone_path(&path) {
            true => Some(Self::get_zone_from_path(path)?),
            false => None,
        })
    }

    fn get_zone_from_path(path: &Path) -> anyhow::Result<PowerZone> {
        let name_path = path.join("name");
        let name = fs::read_to_string(&name_path)?.trim().to_owned();
        let socket_id = match Self::socket_id_from_name(&name)? {
            Some(socket_id) => Some(socket_id),
            None => {
                if let Some(parent_path) = path.parent() {
                    let parent_name_path = parent_path.join("name");
                    if fs::exists(&parent_name_path)? {
                        let parent_name = fs::read_to_string(&parent_name_path)?.trim().to_owned();
                        match Self::socket_id_from_name(&parent_name)? {
                            Some(socket_id) => Some(socket_id),
                            None => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };
        let domain =
            Self::domain_from_name(&name).with_context(|| format!("Unknown RAPL powercap zone {name}"))?;
        Ok(PowerZone {
            name,
            domain,
            path: path.to_path_buf(),
            socket_id,
        })
    }

    fn is_zone_path(path: &Path) -> bool {
        let file_name = path.file_name().unwrap().to_string_lossy();
        path.is_dir() && file_name.starts_with(POWER_ZONE_PREFIX)
    }

    fn socket_id_from_name(name: &str) -> anyhow::Result<Option<u32>> {
        match name.strip_prefix("package-") {
            Some(id_str) => {
                let id: u32 = id_str
                    .parse()
                    .with_context(|| format!("Failed to extract package id from '{name}'"))?;
                Ok(Some(id))
            }
            None => Ok(None),
        }
    }

    fn domain_from_name(name: &str) -> Option<RaplDomainType> {
        match name {
            "psys" => Some(RaplDomainType::Platform),
            "core" => Some(RaplDomainType::PP0),
            "uncore" => Some(RaplDomainType::PP1),
            "dram" => Some(RaplDomainType::Dram),
            _ if name.starts_with("package-") => Some(RaplDomainType::Package),
            _ => None,
        }
    }
}

impl PowerZone {
    pub fn energy_path(&self) -> PathBuf {
        self.path.join("energy_uj")
    }

    pub fn max_energy_path(&self) -> PathBuf {
        self.path.join("max_energy_range_uj")
    }

    fn fmt_rec(&self, f: &mut std::fmt::Formatter<'_>, level: i8) -> std::fmt::Result {
        let mut indent = "  ".repeat(level as _);
        if level > 0 {
            indent.insert(0, '\n');
        }

        let powercap_name = &self.name;
        let domain = self.domain;
        let path = self.path.to_string_lossy();

        write!(f, "{indent}- {powercap_name} ({domain:?}) \t\t: {path}")?;
        Ok(())
    }

    /// creates a new OpenedPowerZone from Self by opening the collection file
    fn open(&self) -> anyhow::Result<OpenedPowerZone> {
        let file = File::open(self.energy_path()).with_context(|| {
            format!(
                "Could not open {}. {PERMISSION_ADVICE}",
                self.energy_path().to_string_lossy()
            )
        })?;

        let str_max_energy_uj = fs::read_to_string(self.max_energy_path()).with_context(|| {
            format!(
                "Could not read {}. {PERMISSION_ADVICE}",
                self.max_energy_path().to_string_lossy()
            )
        })?;

        let max_energy_uj = str_max_energy_uj
            .trim_end()
            .parse()
            .with_context(|| format!("parse max_energy_uj: '{str_max_energy_uj}'"))?;

        let socket = self.socket_id.unwrap_or(0); // put psys in socket 0

        let counter = CounterDiff::with_max_value(max_energy_uj);
        Ok(OpenedPowerZone {
            file,
            domain: self.domain,
            resource: self.domain.to_resource(socket),
            counter,
        })
    }
}

impl Display for PowerZone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_rec(f, 0)
    }
}

impl OpenedPowerZone {
    fn read_counter_diff_in_joules(&mut self, self_reading_buf: &mut Vec<u8>) -> anyhow::Result<Option<f64>> {
        // convert to joules and push
        match self.read_counter_diff(self_reading_buf)? {
            Some(diff) => Ok(Some((diff as f64) * POWERCAP_ENERGY_UNIT)),
            None => Ok(None),
        }
    }

    fn read_counter_diff(&mut self, self_reading_buf: &mut Vec<u8>) -> anyhow::Result<Option<u64>> {
        let energy_uj_value = self.read_counter_value(self_reading_buf)?;
        // store the value, handle the overflow if there is one
        Ok(match self.counter.update(energy_uj_value) {
            CounterDiffUpdate::FirstTime => None,
            CounterDiffUpdate::Difference(diff) => Some(diff),
            CounterDiffUpdate::CorrectedDifference(diff) => {
                log::debug!("Overflow on powercap counter for RAPL domain {}", self.domain);
                Some(diff)
            }
        })
    }

    fn read_counter_value(&mut self, self_reading_buf: &mut Vec<u8>) -> anyhow::Result<u64> {
        // read the file from the beginning
        self.file
            .rewind()
            .with_context(|| format!("failed to rewind {:?}", self.file))?;
        self.file
            .read_to_end(self_reading_buf)
            .with_context(|| format!("failed to read {:?}", self.file))?;

        // parse the content of the file
        let content = std::str::from_utf8(&self_reading_buf)?;
        content
            .trim_end()
            .parse()
            .with_context(|| format!("failed to parse {:?}: '{content}'", self.file))
    }
}

impl PowercapProbe {
    /// creates a new PowercapProbe by passing an Alumet metric ID for energy measurement and related power zones
    pub fn new(metric: TypedMetricId<f64>, zones: &Vec<PowerZone>) -> anyhow::Result<PowercapProbe> {
        if zones.is_empty() {
            return Err(anyhow!("At least one power zone is required for PowercapProbe"))?;
        }

        let mut opened = Vec::with_capacity(zones.len());
        for zone in zones {
            match zone.open() {
                Ok(opened_zone) => opened.push(opened_zone),
                Err(e) => {
                    Self::handle_insufficient_privileges(&e);
                    return Err(e);
                }
            }
        }

        Ok(PowercapProbe { metric, zones: opened })
    }

    fn handle_insufficient_privileges(e: &anyhow::Error) {
        let msg = indoc::formatdoc! {"
            I could not use the powercap sysfs to read RAPL energy counters: {e}.
            This is probably caused by insufficient privileges.
            Please check that you have read access to everything in '{POWERCAP_RAPL_PATH}'.
        
            A solution could be:
                sudo chmod a+r -R {POWERCAP_RAPL_PATH}"};
        log::error!("{msg}");
    }
}

impl alumet::pipeline::Source for PowercapProbe {
    fn poll(&mut self, measurements: &mut MeasurementAccumulator, timestamp: Timestamp) -> Result<(), PollError> {
        // Reuse the same buffer for all the zones.
        // The size of the content of the file `energy_uj` should never exceed those of `max_energy_uj`,
        // which is 16 bytes on all our test machines (if it does exceed 16 bytes it's fine, but less optimal).
        let mut zone_reading_buf = Vec::with_capacity(16);

        for zone in &mut self.zones {
            match zone.read_counter_diff_in_joules(&mut zone_reading_buf)? {
                Some(joules) => {
                    let consumer = ResourceConsumer::LocalMachine;
                    measurements.push(
                        MeasurementPoint::new(timestamp, self.metric, zone.resource.clone(), consumer, joules)
                            .with_attr("domain", AttributeValue::String(zone.domain.to_string())),
                    )
                }
                None => (),
            }
            // clear the buffer, so that we can fill it again
            zone_reading_buf.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::{self, File};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::{tempdir, Builder};

    /// Entry to be created in the mock filesystem
    pub enum EntryType<'a> {
        File(&'a str), // File with content
        Dir,           // Directory
    }

    /// Single entry specification
    pub struct Entry<'a> {
        pub path: &'a str,
        pub entry_type: EntryType<'a>,
    }

    /// Create all specified entries under the given base path
    pub fn create_mock_layout(base_path: PathBuf, entries: &[Entry]) -> std::io::Result<()> {
        for entry in entries {
            let full_path = base_path.join(entry.path);
            match &entry.entry_type {
                EntryType::Dir => fs::create_dir_all(&full_path)?,
                EntryType::File(content) => {
                    if let Some(parent) = full_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    let mut file = File::create(full_path)?;
                    file.write_all(content.as_bytes())?;
                }
            }
        }
        Ok(())
    }

    fn create_valid_mock() -> anyhow::Result<PathBuf> {
        // TO UNCOMMENT - DEBUG PURPOSE
        //let tmp = tempdir()?;
        let tmp = Builder::new().disable_cleanup(true).tempdir()?;
        let base_path = tmp.keep();

        use EntryType::*;

        let entries = [
            Entry {
                path: "enabled",
                entry_type: File("1"),
            },
            Entry {
                path: "intel-rapl:0",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/name",
                entry_type: File("package-0"),
            },
            Entry {
                path: "intel-rapl:0/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/energy_uj",
                entry_type: File("124599532281"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/name",
                entry_type: File("core"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/energy_uj",
                entry_type: File("23893449269"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/name",
                entry_type: File("uncore"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/energy_uj",
                entry_type: File("23992349269"),
            },
            Entry {
                path: "intel-rapl:1",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:1/name",
                entry_type: File("psys"),
            },
            Entry {
                path: "intel-rapl:1/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:1/energy_uj",
                entry_type: File("154571208422"),
            },
            Entry {
                path: "intel-rapl:2",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:2/name",
                entry_type: File("dram"),
            },
            Entry {
                path: "intel-rapl:2/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:2/energy_uj",
                entry_type: File("182178908522"),
            },
        ];

        create_mock_layout(base_path.clone(), &entries)?;
        Ok(base_path)
    }

    #[test]
    fn test_opened_zone_energy_uj_counter_read() -> anyhow::Result<()> {
        let tmp = Builder::new().disable_cleanup(true).tempdir()?;
        let base_path = tmp.keep();

        use EntryType::*;

        let entries = [
            Entry {
                path: "enabled",
                entry_type: File("1"),
            },
            Entry {
                path: "intel-rapl:0",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/name",
                entry_type: File("package-0"),
            },
            Entry {
                path: "intel-rapl:0/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/energy_uj",
                entry_type: File("124599532281"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/name",
                entry_type: File("core"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/energy_uj",
                entry_type: File("23893449269"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/name",
                entry_type: File("uncore"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/energy_uj",
                entry_type: File("23992349269"),
            },
            Entry {
                path: "intel-rapl:1",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:1/name",
                entry_type: File("psys"),
            },
            Entry {
                path: "intel-rapl:1/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:1/energy_uj",
                entry_type: File("154571208422"),
            },
            Entry {
                path: "intel-rapl:2",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:2/name",
                entry_type: File("dram"),
            },
            Entry {
                path: "intel-rapl:2/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:2/energy_uj",
                entry_type: File("212143328850"),
            },
        ];

        create_mock_layout(base_path.clone(), &entries)?;
        let power_zones = all_power_zones_from_path(base_path.as_path())?;

        let mut zone_reading_buf = Vec::with_capacity(16);

        let mut psys_zone = power_zones[3].open()?;
        let mut dram_zone = power_zones[4].open()?;
        let mut core_zone = power_zones[1].open()?;
        let mut uncore_zone = power_zones[2].open()?;
        let mut package_0_zone = power_zones[0].open()?;
        assert_eq!(psys_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, None);
        zone_reading_buf.clear();
        assert_eq!(dram_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, None);
        zone_reading_buf.clear();
        assert_eq!(core_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, None);
        zone_reading_buf.clear();
        assert_eq!(uncore_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, None);
        zone_reading_buf.clear();
        assert_eq!(package_0_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, None);

        let entries = [
            Entry {
                path: "enabled",
                entry_type: File("1"),
            },
            Entry {
                path: "intel-rapl:0",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/name",
                entry_type: File("package-0"),
            },
            Entry {
                path: "intel-rapl:0/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/energy_uj",
                entry_type: File("129999532281"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/name",
                entry_type: File("core"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:0/energy_uj",
                entry_type: File("24293449269"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/name",
                entry_type: File("uncore"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:0/intel-rapl:0:1/energy_uj",
                entry_type: File("23992349269"),
            },
            Entry {
                path: "intel-rapl:1",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:1/name",
                entry_type: File("psys"),
            },
            Entry {
                path: "intel-rapl:1/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:1/energy_uj",
                entry_type: File("154581208422"),
            },
            Entry {
                path: "intel-rapl:2",
                entry_type: Dir,
            },
            Entry {
                path: "intel-rapl:2/name",
                entry_type: File("dram"),
            },
            Entry {
                path: "intel-rapl:2/max_energy_range_uj",
                entry_type: File("262143328850"),
            },
            Entry {
                path: "intel-rapl:2/energy_uj",
                entry_type: File("908522"),
            },
        ];

        create_mock_layout(base_path.clone(), &entries)?;

        zone_reading_buf.clear();
        assert_eq!(psys_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, Some(10.0));
        zone_reading_buf.clear();
        assert_eq!(
            dram_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?,
            Some(50000.908523)
        ); // overflow / corrected difference case
        zone_reading_buf.clear();
        assert_eq!(core_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, Some(400.0));
        zone_reading_buf.clear();
        assert_eq!(uncore_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?, Some(0.0));
        zone_reading_buf.clear();
        assert_eq!(
            package_0_zone.read_counter_diff_in_joules(&mut zone_reading_buf)?,
            Some(5400.0)
        );

        Ok(())
    }

    #[test]
    fn test_opened_zone_energy_uj_read() -> anyhow::Result<()> {
        let base_path = create_valid_mock()?;
        let power_zones = all_power_zones_from_path(base_path.as_path())?;
        let mut zone_reading_buf = Vec::with_capacity(16);
        let mut psys_zone = power_zones[3].open()?;
        let mut dram_zone = power_zones[4].open()?;
        let mut core_zone = power_zones[1].open()?;
        let mut uncore_zone = power_zones[2].open()?;
        let mut package_0_zone = power_zones[0].open()?;
        assert_eq!(package_0_zone.read_counter_value(&mut zone_reading_buf)?, 124599532281);
        zone_reading_buf.clear();
        assert_eq!(core_zone.read_counter_value(&mut zone_reading_buf)?, 23893449269);
        zone_reading_buf.clear();
        assert_eq!(uncore_zone.read_counter_value(&mut zone_reading_buf)?, 23992349269);
        zone_reading_buf.clear();
        assert_eq!(psys_zone.read_counter_value(&mut zone_reading_buf)?, 154571208422);
        zone_reading_buf.clear();
        assert_eq!(dram_zone.read_counter_value(&mut zone_reading_buf)?, 182178908522);
        Ok(())
    }

    #[test]
    fn test_all_power_zones_from_path() -> anyhow::Result<()> {
        let base_path = create_valid_mock()?;
        let base_str = base_path.to_str().expect("cannot convert base_path to str");

        let actual_top_zones = all_power_zones_from_path(base_path.as_path())?;

        let expected_top_zones = vec![
            PowerZone {
                name: "package-0".to_string(),
                domain: RaplDomainType::Package,
                path: PathBuf::from(format!("{}/intel-rapl:0", base_str)),
                socket_id: Some(0),
            },
            PowerZone {
                name: "core".to_string(),
                domain: RaplDomainType::PP0,
                path: PathBuf::from(format!("{}/intel-rapl:0/intel-rapl:0:0", base_str)),
                socket_id: Some(0),
            },
            PowerZone {
                name: "uncore".to_string(),
                domain: RaplDomainType::PP1,
                path: PathBuf::from(format!("{}/intel-rapl:0/intel-rapl:0:1", base_str)),
                socket_id: Some(0),
            },
            PowerZone {
                name: "psys".to_string(),
                domain: RaplDomainType::Platform,
                path: PathBuf::from(format!("{}/intel-rapl:1", base_str)),
                socket_id: None,
            },
            PowerZone {
                name: "dram".to_string(),
                domain: RaplDomainType::Dram,
                path: PathBuf::from(format!("{}/intel-rapl:2", base_str)),
                socket_id: None,
            },
        ];

        assert_eq!(actual_top_zones, expected_top_zones);

        Ok(())
    }
}
