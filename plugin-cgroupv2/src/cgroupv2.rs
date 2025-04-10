use alumet::{
    metrics::{error::MetricCreationError, TypedMetricId},
    plugin::AlumetPluginStart,
    units::{PrefixedUnit, Unit},
    resources::{ResourceConsumer},
};
use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{Read, Seek}
};

pub(crate) const CGROUP_MAX_TIME_COUNTER: u64 = u64::MAX;

#[derive(Debug, PartialEq, Clone)]
pub struct CgroupMeasurements {
    /// Total CPU usage time by the cgroup.
    pub cpu_time_total: u64,
    /// CPU in user mode usage time by the cgroup.
    pub cpu_time_user_mode: u64,
    /// CPU in system mode usage time by the cgroup.
    pub cpu_time_system_mode: u64,
    /// Resident memory usage (RSS) currently used by the cgroup.
    pub memory_usage_resident: u64,
    /// Anonymous used memory, corresponding to running process and various allocated memory.
    pub memory_anonymous: u64,
    // Files memory, corresponding to open files and descriptors.
    pub memory_file: u64,
    // Memory reserved for kernel operations.
    pub memory_kernel: u64,
    /// Memory used to manage correspondence between virtual and physical addresses.
    pub memory_pagetables: u64,
}

pub struct CgroupMeasurer {
    name: String,
    cpu_stats_file: File,
    memory_stats_file: File,
    memory_current_file: File,
    pub cpu_stats_consumer: ResourceConsumer,
    pub memory_stats_consumer: ResourceConsumer,
    pub memory_current_consumer: ResourceConsumer,
}

impl CgroupMeasurer {
    pub fn new(
        name: String,
        cgroup_path: String,
    ) -> anyhow::Result<Self> {
        let cpu_stats_file_path = format!("{}/cpu.stat", cgroup_path.clone());
        let memory_stats_file_path = format!("{}/memory.stat", cgroup_path.clone());
        let memory_current_file_path = format!("{}/memory.current", cgroup_path.clone());
        Ok(CgroupMeasurer{
            name: name,
            cpu_stats_file: File::open(&cpu_stats_file_path).with_context(|| format!("failed to open file {}", cpu_stats_file_path))?,
            memory_stats_file: File::open(&memory_stats_file_path).with_context(|| format!("failed to open file {}", memory_stats_file_path))?,
            memory_current_file: File::open(&memory_current_file_path).with_context(|| format!("failed to open file {}", memory_current_file_path))?,
            cpu_stats_consumer: ResourceConsumer::ControlGroup {
                path: cpu_stats_file_path.into(),
            },
            memory_stats_consumer: ResourceConsumer::ControlGroup {
                path: memory_stats_file_path.into(),
            },
            memory_current_consumer: ResourceConsumer::ControlGroup {
                path: memory_current_file_path.into(),
            },
        })
    }

    pub fn measure(&mut self) -> anyhow::Result<CgroupMeasurements> { 
        let mut cgroup_measurements = CgroupMeasurements::new();
        let mut content_buffer = String::new();

        CgroupMeasurer::_measure(
            &mut cgroup_measurements,
            "cpu.stat".to_string(),
            &mut self.cpu_stats_file,
            &mut content_buffer,
            |cgroup_measurements: &mut CgroupMeasurements, content_buffer: &mut String| -> Result<()> {
                cgroup_measurements.load_from_cpu_stat(content_buffer)
            },
        )?;

        CgroupMeasurer::_measure(
            &mut cgroup_measurements,
            "memory.stat".to_string(),
            &mut self.memory_stats_file,
            &mut content_buffer,
            |cgroup_measurements: &mut CgroupMeasurements, content_buffer: &mut String| -> Result<()> {
                cgroup_measurements.load_from_memory_stat(content_buffer)
            },
        )?;

        CgroupMeasurer::_measure(
            &mut cgroup_measurements,
            "memory.current".to_string(),
            &mut self.memory_current_file,
            &mut content_buffer,
            |cgroup_measurements: &mut CgroupMeasurements, content_buffer: &mut String| -> Result<()> {
                cgroup_measurements.load_from_memory_current(content_buffer)
            },
        )?;
        Ok(cgroup_measurements)
    }

    fn _measure<Decoder>(
        cgroup_measurements: &mut CgroupMeasurements,
        name: String,
        file: &mut File,
        content_buffer: &mut String,
        mut decode: Decoder
    )-> anyhow::Result<()>
    where
        Decoder: FnMut(&mut CgroupMeasurements, &mut String) -> Result<()>,
    {
        file.rewind()?;
        content_buffer.clear();
        file.read_to_string(content_buffer)
            .context(format!("unable to read {} file", name))?;

        if content_buffer.is_empty() {
            return Err(anyhow::anyhow!("{} file is empty for", name));
        }

        decode(cgroup_measurements, content_buffer)?;
        Ok(())
    }
}

impl CgroupMeasurements {
    pub fn new() -> Self {
        CgroupMeasurements {
            cpu_time_total: 0,
            cpu_time_user_mode: 0,
            cpu_time_system_mode: 0,
            memory_usage_resident: 0,
            memory_anonymous: 0,
            memory_file: 0,
            memory_kernel: 0,
            memory_pagetables: 0,
        }
    }

    /// load_from_str loads the CgroupMeasurements structure from cgroupv2 "memory.stat" file
    pub fn load_from_cpu_stat(&mut self, content: &String) -> anyhow::Result<()> {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_ascii_whitespace().collect();
            if parts.len() >= 2 {
                let value = parts[1]
                    .parse::<u64>()
                    .with_context(|| format!("Parsing of value : {}", parts[1]))?;
                match parts[0] {
                    "usage_usec" => self.cpu_time_total = value,
                    "user_usec" => self.cpu_time_user_mode = value,
                    "system_usec" => self.cpu_time_system_mode = value,
                    _ => continue,
                }
            }
        }
        Ok(())
    }

    /// load_from_str loads the CgroupMeasurements structure from cgroupv2 "memory.stat" file
    pub fn load_from_memory_stat(&mut self, content: &String) -> anyhow::Result<()> {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_ascii_whitespace().collect();
            if parts.len() >= 2 {
                let value = parts[1]
                    .parse::<u64>()
                    .with_context(|| format!("Parsing of value : {}", parts[1]))?;
                match parts[0] {
                    "anon" => self.memory_anonymous = value,
                    "file" => self.memory_file = value,
                    "kernel_stack" => self.memory_kernel = value,
                    "pagetables" => self.memory_pagetables = value,
                    _ => continue,
                }
            }
        }
        Ok(())
    }

    /// load_from_str loads the CgroupMeasurements structure from cgroupv2 "memory.stat" file
    pub fn load_from_memory_current(&mut self, content: &String) -> anyhow::Result<()> {
        self.memory_usage_resident = content.as_str().parse::<u64>().with_context(|| format!("Parsing of value : {}", content))?;
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Metrics {
    /// Total CPU usage time by the cgroup since last measurement
    pub cpu_time_delta: TypedMetricId<u64>,
    /// memory currently used by the cgroup.
    pub memory_usage: TypedMetricId<u64>,
    /// Anonymous used memory, corresponding to running process and various allocated memory.
    pub memory_anonymous: TypedMetricId<u64>,
    /// Files memory, corresponding to open files and descriptors.
    pub memory_file: TypedMetricId<u64>,
    /// Memory reserved for kernel operations.
    pub memory_kernel: TypedMetricId<u64>,
    /// Memory used to manage correspondence between virtual and physical addresses.
    pub memory_pagetables: TypedMetricId<u64>,
    /// Total memory used by cgroup.
    pub memory_total: TypedMetricId<u64>,
}

impl Metrics {
    /// Provides a information base to create metric before sending CPU and memory data,
    /// with `name`, `unit` and `description` parameters.
    ///
    /// # Arguments
    ///
    /// * `alumet` - A AlumetPluginStart structure passed to plugins for the start-up phase.
    ///
    /// # Error
    ///
    ///  Return `MetricCreationError` when an error occur during creation a new metric.
    pub fn new(alumet: &mut AlumetPluginStart) -> Result<Self, MetricCreationError> {
        Ok(Self {
            cpu_time_delta: alumet.create_metric::<u64>(
                "cpu_time_delta",
                PrefixedUnit::nano(Unit::Second),
                "Total CPU usage time by the cgroup since last measurement",
            )?,

            // Memory cgroup data
            memory_usage: alumet.create_metric::<u64>(
                "memory_usage",
                Unit::Byte.clone(),
                "Memory currently used by the cgroup.",
            )?,
            memory_anonymous: alumet.create_metric::<u64>(
                "cgroup_memory_anonymous",
                Unit::Byte.clone(),
                "Anonymous used memory, corresponding to running process and various allocated memory",
            )?,
            memory_file: alumet.create_metric::<u64>(
                "cgroup_memory_file",
                Unit::Byte.clone(),
                "Files memory, corresponding to open files and descriptors",
            )?,
            memory_kernel: alumet.create_metric::<u64>(
                "cgroup_memory_kernel_stack",
                Unit::Byte.clone(),
                "Memory reserved for kernel operations",
            )?,
            memory_pagetables: alumet.create_metric::<u64>(
                "cgroup_memory_pagetables",
                Unit::Byte.clone(),
                "Memory used to manage correspondence between virtual and physical addresses",
            )?,
            memory_total: alumet.create_metric::<u64>(
                "cgroup_memory_total",
                Unit::Byte.clone(),
                "Total memory used by cgroup",
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test `from_str` function with in extracted result,
    // a negative value to test representation
    #[test]
    fn test_signed_values() {
        let str_cpu = "
            usage_usec -10000
            user_usec -20000
            system_usec -30000";

        let str_memory = "
            anon -10000
            file -20000
            kernel_stack -30000
            pagetables -40000
            percpu 890784
            sock 16384
            shmem 2453504
            file_mapped -50000
            ....";

        CgroupMeasurements::from_str(str_cpu).expect_err("ERROR : Signed value");
        CgroupMeasurements::from_str(str_memory).expect_err("ERROR : Signed value");
    }

    // Test `from_str` function with in extracted result,
    // a float or decimal value
    #[test]
    fn test_double_values() {
        let str_cpu = "
            usage_usec 10000.05
            user_usec 20000.25
            system_usec 30000.33";

        let str_memory = "
            anon 10000.05
            file 20000.25
            kernel_stack 30000.33
            pagetables 124325768932.56";

        CgroupMeasurements::from_str(str_cpu).expect_err("ERROR : Decimal value");
        CgroupMeasurements::from_str(str_memory).expect_err("ERROR : Decimal value");
    }

    // Test `from_str` function with in extracted result,
    // a null, empty or incompatible string
    #[test]
    fn test_invalid_values() {
        let str_cpu = "
            usage_usec !#⚠
            user_usec
            system_usec -123abc";

        let str_memory = "
            anon !#⚠
            file
            pagetables -123abc
            ...";

        CgroupMeasurements::from_str(str_cpu).expect_err("ERROR : Incompatible value");
        CgroupMeasurements::from_str(str_memory).expect_err("ERROR : Incompatible value");
    }

    // Test `from_str` function with in extracted result,
    // an empty string
    #[test]
    fn test_empty_values() {
        let str: &str = "";
        let result = CgroupMeasurements::from_str(str).unwrap();
        // Memory file str
        assert_eq!(result.memory_anonymous, 0);
        assert_eq!(result.memory_file, 0);
        assert_eq!(result.memory_kernel, 0);
        assert_eq!(result.memory_pagetables, 0);
        // CPU file str
        assert_eq!(result.cpu_time_total, 0);
        assert_eq!(result.cpu_time_user_mode, 0);
        assert_eq!(result.cpu_time_system_mode, 0);
    }

    // Test for calculating `mem_total` with structure parameters
    #[test]
    fn test_calc_mem() {
        let result = CgroupMeasurements {
            pod_name: "".to_owned(),
            pod_uid: "test_pod_uid".to_owned(),
            namespace: "test_pod_namespace".to_owned(),
            node: "test_pod_node".to_owned(),
            cpu_time_total: 64,
            cpu_time_user_mode: 16,
            cpu_time_system_mode: 32,
            memory_anonymous: 1024,
            memory_file: 256,
            memory_kernel: 4096,
            memory_pagetables: 512,
        };

        let expected = CgroupMeasurements {
            pod_name: "".to_owned(),
            pod_uid: "test_pod_uid".to_owned(),
            namespace: "test_pod_namespace".to_owned(),
            node: "test_pod_node".to_owned(),
            cpu_time_total: 64,
            cpu_time_user_mode: 16,
            cpu_time_system_mode: 32,
            memory_anonymous: 1024,
            memory_file: 256,
            memory_kernel: 4096,
            memory_pagetables: 512,
        };

        assert_eq!(result, expected);

        let mem_total = result.memory_anonymous + result.memory_file + result.memory_kernel + result.memory_pagetables;
        assert_eq!(mem_total, 5888);
    }
}
