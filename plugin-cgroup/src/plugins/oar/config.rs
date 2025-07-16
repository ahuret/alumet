use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub(crate) oar_version: OarVersion,
    #[serde(with = "humantime_serde")]
    pub(crate) poll_interval: Duration,
    pub(crate) jobs_only: bool,

    /// Set to true if you want to start the source in Pause mode. If so it will need to be resumed to poll measurements.
    /// This is useful for scenario where you dynamically want to start/stop the poll mechanism
    pub(crate) initial_state_to_pause: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            oar_version: OarVersion::Oar3,
            poll_interval: Duration::from_secs(1),
            jobs_only: true,
            initial_state_to_pause: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OarVersion {
    Oar2,
    Oar3,
}
