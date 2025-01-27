use crate::{agent, measurement::MeasurementBuffer, metrics::Metric, plugin::PluginMetadata};

pub struct RuntimeExpectations {}

impl RuntimeExpectations {
    pub fn new(){
        todo!()
    }

    pub fn build() -> PluginMetadata {
        todo!()
    }

    pub fn source_output(self, source_name: &str, f: impl Fn(&MeasurementBuffer)) -> Self{
        todo!()
    }

    pub fn source_result(self, source_name: &str, prepare: impl Fn(), check: impl Fn(&MeasurementBuffer)) -> Self{
        todo!()
    }

    pub fn transform_output(self, source_name: &str, f: impl Fn(&MeasurementBuffer)) -> Self{
        todo!()
    }

    pub fn transform_result(self, source_name: &str, input: impl Fn(&mut MeasurementBuffer), output: impl Fn(&MeasurementBuffer)) -> Self{
        todo!()
    }

}

pub struct StartupExpectations {}

impl StartupExpectations {
    pub fn new() -> Self{
        todo!()
    }
    
    pub(crate) fn apply(self, builder: &mut agent::Builder) {
        todo!()
    }

    pub fn start_metric(self, metric_name: &str, metric: Metric) -> Self {
        todo!()
    }

    pub fn element_source(self, plugin_name: &str, source_name: &str, source_type: SourceType) -> Self {
        todo!()
    }

    pub fn element_transform(self, plugin_name: &str, transform_name: &str) -> Self {
        todo!()
    }

    pub fn element_output(self, plugin_name: &str, output_name: &str) -> Self {
        todo!()
    }

}
