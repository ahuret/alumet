use alumet::{
    measurement::{AttributeValue, MeasurementAccumulator, MeasurementPoint, Timestamp},
    metrics::TypedMetricId,
    pipeline::elements::source::error::{PollError, PollRetry},
    plugin::util::CounterDiff,
    resources::{Resource, ResourceConsumer},
};
use anyhow::{Context, Result};

use super::utils::{WatchedCgroup};
use crate::cgroupv2::{CgroupMeasurements, Metrics};

pub struct K8SProbe {
    pub watched_cgroup: WatchedCgroup,
    pub time_tot: CounterDiff,
    pub time_usr: CounterDiff,
    pub time_sys: CounterDiff,
    pub cpu_time_delta: TypedMetricId<u64>,
    pub memory_usage: TypedMetricId<u64>,
    pub memory_anon: TypedMetricId<u64>,
    pub memory_file: TypedMetricId<u64>,
    pub memory_kernel: TypedMetricId<u64>,
    pub memory_pagetables: TypedMetricId<u64>,
    pub memory_total: TypedMetricId<u64>,
}

impl K8SProbe {
    pub fn new(
        metric: Metrics,
        watched_cgroup: WatchedCgroup,
        counter_tot: CounterDiff,
        counter_sys: CounterDiff,
        counter_usr: CounterDiff,
    ) -> anyhow::Result<K8SProbe> {
        Ok(K8SProbe {
            watched_cgroup: watched_cgroup,
            time_tot: counter_tot,
            time_usr: counter_usr,
            time_sys: counter_sys,
            cpu_time_delta: metric.cpu_time_delta,
            memory_usage: metric.memory_usage,
            memory_anon: metric.memory_anonymous,
            memory_file: metric.memory_file,
            memory_kernel: metric.memory_kernel,
            memory_pagetables: metric.memory_pagetables,
            memory_total: metric.memory_total,
        })
    }
}

impl alumet::pipeline::Source for K8SProbe {
    fn poll(&mut self, measurements: &mut MeasurementAccumulator, timestamp: Timestamp) -> Result<(), PollError> {
        /// Create a measurement point with given value,
        /// the `LocalMachine` resource and some attributes related to the pod.
        fn create_measurement_point(
            timestamp: Timestamp,
            metric_id: TypedMetricId<u64>,
            resource_consumer: ResourceConsumer,
            value_measured: u64,
            pod_uid: String,
            pod_name: String,
            namespace: String,
            node: String,
        ) -> MeasurementPoint {
            MeasurementPoint::new(
                timestamp,
                metric_id,
                Resource::LocalMachine,
                resource_consumer,
                value_measured,
            )
            .with_attr("uid", AttributeValue::String(pod_uid))
            .with_attr("name", AttributeValue::String(pod_name))
            .with_attr("namespace", AttributeValue::String(namespace))
            .with_attr("node", AttributeValue::String(node))
        }

        let metrics = self.watched_cgroup.measurer.measure()
            .context("Error get value")
            .retry_poll()?;

        let diff_tot = self.time_tot.update(metrics.cpu_time_total).difference();
        let diff_usr = self.time_usr.update(metrics.cpu_time_user_mode).difference();
        let diff_sys = self.time_sys.update(metrics.cpu_time_system_mode).difference();

        // Push cpu total usage measure for user and system
        if let Some(value_tot) = diff_tot {
            let p_tot = create_measurement_point(
                timestamp,
                self.cpu_time_delta,
                self.watched_cgroup.measurer.cpu_stats_consumer.clone(),
                value_tot,
                self.watched_cgroup.uid.clone(),
                self.watched_cgroup.name.clone(),
                self.watched_cgroup.namespace.clone(),
                self.watched_cgroup.node.clone(),
            )
            .with_attr("kind", "total");
            measurements.push(p_tot);
        }

        // Push cpu usage measure for user
        if let Some(value_usr) = diff_usr {
            let p_usr = create_measurement_point(
                timestamp,
                self.cpu_time_delta,
                self.watched_cgroup.measurer.cpu_stats_consumer.clone(),
                value_usr,
                self.watched_cgroup.uid.clone(),
                self.watched_cgroup.name.clone(),
                self.watched_cgroup.namespace.clone(),
                self.watched_cgroup.node.clone(),
            )
            .with_attr("kind", "user");
            measurements.push(p_usr);
        }

        // Push cpu usage measure for system
        if let Some(value_sys) = diff_sys {
            let p_sys = create_measurement_point(
                timestamp,
                self.cpu_time_delta,
                self.watched_cgroup.measurer.cpu_stats_consumer.clone(),
                value_sys,
                self.watched_cgroup.uid.clone(),
                self.watched_cgroup.name.clone(),
                self.watched_cgroup.namespace.clone(),
                self.watched_cgroup.node.clone(),
            )
            .with_attr("kind", "system");
            measurements.push(p_sys);
        }

        // Push anonymous used memory measure corresponding to running process and various allocated memory
        let mem_usage_resident_value = metrics.memory_usage_resident;
        let m_usage = create_measurement_point(
            timestamp,
            self.memory_usage,
            self.watched_cgroup.measurer.memory_stats_consumer.clone(),
            mem_usage_resident_value,
            self.watched_cgroup.uid.clone(),
            self.watched_cgroup.name.clone(),
            self.watched_cgroup.namespace.clone(),
            self.watched_cgroup.node.clone(),
        ).with_attr("kind", "resident");
        measurements.push(m_usage);

        // Push anonymous used memory measure corresponding to running process and various allocated memory
        let mem_anon_value = metrics.memory_anonymous;
        let m_anon = create_measurement_point(
            timestamp,
            self.memory_anon,
            self.watched_cgroup.measurer.memory_stats_consumer.clone(),
            mem_anon_value,
            self.watched_cgroup.uid.clone(),
            self.watched_cgroup.name.clone(),
            self.watched_cgroup.namespace.clone(),
            self.watched_cgroup.node.clone(),
        );
        measurements.push(m_anon);

        // Push files memory measure, corresponding to open files and descriptors
        let mem_file_value = metrics.memory_file;
        let m_file = create_measurement_point(
            timestamp,
            self.memory_file,
            self.watched_cgroup.measurer.memory_stats_consumer.clone(),
            mem_file_value,
            self.watched_cgroup.uid.clone(),
            self.watched_cgroup.name.clone(),
            self.watched_cgroup.namespace.clone(),
            self.watched_cgroup.node.clone(),
        );
        measurements.push(m_file);

        // Push kernel memory measure
        let mem_kernel_value = metrics.memory_kernel;
        let m_ker = create_measurement_point(
            timestamp,
            self.memory_kernel,
            self.watched_cgroup.measurer.memory_stats_consumer.clone(),
            mem_kernel_value,
            self.watched_cgroup.uid.clone(),
            self.watched_cgroup.name.clone(),
            self.watched_cgroup.namespace.clone(),
            self.watched_cgroup.node.clone(),
        );
        measurements.push(m_ker);

        // Push pagetables memory measure
        let mem_pagetables_value = metrics.memory_pagetables;
        let m_pgt = create_measurement_point(
            timestamp,
            self.memory_pagetables,
            self.watched_cgroup.measurer.memory_stats_consumer.clone(),
            mem_pagetables_value,
            self.watched_cgroup.uid.clone(),
            self.watched_cgroup.name.clone(),
            self.watched_cgroup.namespace.clone(),
            self.watched_cgroup.node.clone(),
        );
        measurements.push(m_pgt);

        // Push total memory used by cgroup measure
        let mem_total_value = mem_anon_value + mem_file_value + mem_kernel_value + mem_pagetables_value;
        let m_tot = create_measurement_point(
            timestamp,
            self.memory_total,
            self.watched_cgroup.measurer.memory_stats_consumer.clone(),
            mem_total_value,
            self.watched_cgroup.uid.clone(),
            self.watched_cgroup.name.clone(),
            self.watched_cgroup.namespace.clone(),
            self.watched_cgroup.node.clone(),
        );
        measurements.push(m_tot);

        Ok(())
    }
}
