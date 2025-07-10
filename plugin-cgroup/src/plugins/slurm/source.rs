use alumet::pipeline::elements::source::trigger::TriggerSpec;
use util_cgroups::Cgroup;

// use super::OarVersion;
use crate::{
    common::{
        cgroup_events::{CgroupSetupCallback, ProbeSetup, SourceSettings},
        metrics::{AugmentedMetrics, Metrics},
        regex::RegexAttributesExtrator,
    },
    plugins::slurm::attr::{find_jobid_in_attrs, JOB_REGEX_SLURM2},
};

#[derive(Clone)]
pub struct JobSourceSetup {
    extractor: RegexAttributesExtrator,
    trigger: TriggerSpec,
    jobs_only: bool,
}

impl JobSourceSetup {
    pub fn new(config: super::Config) -> anyhow::Result<Self> {
        let trigger = TriggerSpec::at_interval(config.poll_interval);
        // match config.oar_version {
        //     SlurmCgroupVersion::V1 => Ok(Self {
        //         extractor: RegexAttributesExtrator::new(JOB_REGEX_SLURM1)?,
        //         trigger,
        //         jobs_only: config.jobs_only,
        //     }),
        //     SlurmCgroupVersion::V2 => Ok(Self {
        //         extractor: RegexAttributesExtrator::new(JOB_REGEX_SLURM2)?,
        //         trigger,
        //         jobs_only: config.jobs_only,
        //     }),
        // }
        Ok(Self {
            extractor: RegexAttributesExtrator::new(JOB_REGEX_SLURM2)?,
            trigger,
            jobs_only: config.jobs_only,
        })
    }
}

impl CgroupSetupCallback for JobSourceSetup {
    fn setup_new_probe(&mut self, cgroup: &Cgroup, metrics: &Metrics) -> Option<ProbeSetup> {
        // extracts attributes "job_id" and ("user" or "user_id")
        let attrs = self
            .extractor
            .extract(cgroup.canonical_path())
            .expect("bad regex: it should only match if the input can be parsed into the specified types");

        let is_job = !attrs.is_empty();
        let name: String;
        

        if is_job {
            let job_id = find_jobid_in_attrs(&attrs).expect("job_id should be set");
            // log::info!("job_id is: {:?}, attrs: {:?}", job_id, attrs);
            // attrs.push((String::from("job_id"), AttributeValue::String(job_id.to_string())));

            // give a nice name
            name = format!(
                "slurm-job-{}",
                job_id
            );
        } else {
            // not a job, just a cgroup (for ex. a systemd service)
            if self.jobs_only {
                return None; // don't measure this cgroup
            }
            name = format!("cgroup {}", cgroup.unique_name());
        }

        let trigger = self.trigger.clone();
        let source_settings = SourceSettings { name, trigger };
        let metrics = AugmentedMetrics::with_common_attr_vec(metrics, attrs);
        Some(ProbeSetup {
            metrics,
            source_settings,
        })
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn test_username_from_id() {
    //     let username = username_from_id(1000).unwrap();
    //     println!("{username}");
    // }
}