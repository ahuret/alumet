//! InfluxDB2 API.

use alumet::measurement::Timestamp;
use reqwest::{header, Url};
use std::{
    borrow::Cow,
    fmt::Write,
    time::{SystemTime, UNIX_EPOCH},
};

/// Client for InfluxDB v2.
pub struct Client {
    client: reqwest::Client,
    /// String of the form `<host>/api/v2/write`.
    write_url: String,
    /// String of the form `Token <api_token>`.
    token_header: String,
}

impl Client {
    pub fn new(host: String, token: String) -> Self {
        let write_url = format!("{host}/api/v2/write");
        let token = format!("Token {token}");
        Self {
            client: reqwest::Client::new(),
            write_url,
            token_header: token,
        }
    }

    /// Writes measurements to InfluxDB, in the given organization and bucket.
    pub async fn write(&self, org: &str, bucket: &str, data: LineProtocolData) -> anyhow::Result<()> {
        // TODO optimize: https://docs.influxdata.com/influxdb/v2/write-data/best-practices/optimize-writes
        let precision = "ns";
        let url = Url::parse_with_params(
            &self.write_url,
            &[("org", org), ("bucket", bucket), ("precision", precision)],
        )?;
        let res = self
            .client
            .post(url.clone())
            .header(header::AUTHORIZATION, &self.token_header)
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(data.0)
            .send()
            .await?;

        res.error_for_status()?;
        Ok(())
    }

    /// Tests whether it is possible to write to the given organization and bucket with the client.
    ///
    /// Returns `Ok(())` if all goes well.
    pub async fn test_write(&self, org: &str, bucket: &str) -> anyhow::Result<()> {
        // send empty data
        self.write(org, bucket, LineProtocolData(String::new())).await
    }
}

#[derive(Debug)]
pub struct LineProtocolData(String);

impl LineProtocolData {
    pub fn builder() -> LineProtocolBuilder {
        LineProtocolBuilder::new()
    }
}

pub struct LineProtocolBuilder {
    buf: String,
    after_first_field: bool,
}

#[allow(unused)]
impl LineProtocolBuilder {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            after_first_field: false,
        }
    }

    #[allow(unused)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: String::with_capacity(capacity),
            after_first_field: false,
        }
    }

    /// Writes the measurement to the current line.
    ///
    /// Must be called first in a line. Required.
    pub fn measurement(&mut self, name: &str) -> &mut Self {
        if self.after_first_field {
            self.after_first_field = false;
            self.buf.push('\n'); // new measurement
        }
        self.buf.push_str(&escape_string(name, &[',', ' ']));
        self
    }

    /// Writes a tag to the current line.
    ///
    /// Must be called after `measurement`. Optional.
    pub fn tag(&mut self, key: &str, value: &str) -> &mut Self {
        // tag values cannot be empty!
        if !value.is_empty() {
            let key = escape_string(key, &[',', '=', ' ']);
            let value = escape_string(value, &[',', '=', ' ']);
            write!(self.buf, ",{key}={value}").unwrap();
        }
        self
    }

    /// Writes a field to the current line.
    ///
    /// Must be called after `tag` (or `measurement` if there's no tag).
    /// Required (there must be at least one field).
    fn field(&mut self, key: &str, serialized_value: &str) -> &mut Self {
        let key = escape_string(key, &[',', '=', ' ']);
        if self.after_first_field {
            write!(self.buf, ",{key}={serialized_value}").unwrap();
        } else {
            write!(self.buf, " {key}={serialized_value}").unwrap();
            self.after_first_field = true;
        }
        self
    }

    /// Writes a field to the current line.
    ///
    /// Must be called after `tag` (or `measurement` if there's no tag).
    /// Required (there must be at least one field).
    pub fn field_float(&mut self, key: &str, value: f64) -> &mut Self {
        self.field(key, &value.to_string())
    }

    /// Writes a field to the current line.
    ///
    /// Must be called after `tag` (or `measurement` if there's no tag).
    /// Required (there must be at least one field).
    pub fn field_int(&mut self, key: &str, value: i64) -> &mut Self {
        self.field(key, &format!("{value}i"))
    }

    /// Writes a field to the current line.
    ///
    /// Must be called after `tag` (or `measurement` if there's no tag).
    /// Required (there must be at least one field).
    pub fn field_uint(&mut self, key: &str, value: u64) -> &mut Self {
        self.field(key, &format!("{value}u"))
    }

    /// Writes a field to the current line.
    ///
    /// Must be called after `tag` (or `measurement` if there's no tag).
    /// Required (there must be at least one field).
    pub fn field_string(&mut self, key: &str, value: &str) -> &mut Self {
        let escaped = escape_string(value, &['"', '\\']);
        self.field(key, &format!("\"{escaped}\""))
    }

    /// Writes a field to the current line.
    ///
    /// Must be called after `tag` (or `measurement` if there's no tag).
    /// Required (there must be at least one field).
    pub fn field_bool(&mut self, key: &str, value: bool) -> &mut Self {
        self.field(key, if value { "T" } else { "F" })
    }

    /// Writes a tag to the current line.
    ///
    /// Must be called after `field`. Required.
    pub fn timestamp(&mut self, timestamp: Timestamp) -> &mut Self {
        let nanoseconds = SystemTime::from(timestamp)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        write!(self.buf, " {nanoseconds}").unwrap();
        self
    }

    pub fn build(self) -> LineProtocolData {
        assert!(
            self.after_first_field,
            "wrong use of the LineProtocolBuilder: at least one field is required"
        );
        LineProtocolData(self.buf)
    }
}

/// Escape a String to make it suitable for the line protocol.
///
/// See https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters.
fn escape_string<'a>(s: &'a str, chars_to_escape: &[char]) -> Cow<'a, str> {
    if s.contains(chars_to_escape) {
        // escape required, allocate a new string
        let mut escaped = String::with_capacity(s.len() + 2);
        for c in s.chars() {
            if chars_to_escape.contains(&c) {
                escaped.push('\\');
            }
            escaped.push(c);
        }
        Cow::Owned(escaped)
    } else {
        // nothing to escape, return the same string without allocating
        Cow::Borrowed(s)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use alumet::measurement::Timestamp;

    use crate::influxdb2::escape_string;

    use super::{Client, LineProtocolBuilder, LineProtocolData};

    #[test]
    fn escaping() {
        assert_eq!("myMeasurement", escape_string("myMeasurement", &['\\', ' ', '=']));
        assert_eq!("with\\ space", escape_string("with space", &['\\', ' ', '=']));
        assert_eq!(
            "with\\ space\\ and\\ backslash\\\\",
            escape_string("with space and backslash\\", &['\\', ' ', '='])
        );
    }

    struct TestedLineProtocolData {
        line: LineProtocolData,
        expected_str: &'static str,
    }

    fn get_tested_lines() -> Vec<TestedLineProtocolData> {
        let mut tested_lines =Vec::new();

        let mut builder = LineProtocolData::builder();
        builder
            .measurement("myMeasurement")
            .tag("tag1", "value1")
            .tag("tag2", "value2")
            .field_string("fieldKey", "fieldValue")
            .timestamp(Timestamp::from(UNIX_EPOCH + Duration::from_nanos(1556813561098000000)));
        let line = builder.build();

        tested_lines.push(TestedLineProtocolData{
            line: line,
            expected_str: r#"myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000"#,
        });

        let mut builder = LineProtocolData::builder();
        builder
            .measurement("myMeasurement")
            .tag("tag1", "value1")
            .tag("tag2", "value2")
            .field_string("fieldKey", "fieldValue")
            .timestamp(Timestamp::from(UNIX_EPOCH + Duration::from_nanos(1556813561098000000)));
        builder
            .measurement("measurement_without_tags")
            .field_string("fieldKey", "fieldValue")
            .field_bool("bool", true)
            .field_float("float", 123.0)
            .field_int("int", -123)
            .field_uint("uint", 123)
            .timestamp(Timestamp::from(UNIX_EPOCH + Duration::from_nanos(1556813561098000000)));
        let line = builder.build();
        tested_lines.push(TestedLineProtocolData{
            line: line,
            expected_str: r#"myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000
measurement_without_tags fieldKey="fieldValue",bool=T,float=123,int=-123i,uint=123u 1556813561098000000"#, 
        });
        tested_lines
    }

    #[test]
    fn build_line() {
        for tested_line in get_tested_lines() {
            assert_eq!(tested_line.line.0, tested_line.expected_str);
        }
    }

    use mockito::{Matcher, Server};

    #[tokio::test]
    async fn write() {
        let mut server = Server::new_async().await;

        let token = "sometoken";
        let token_header = format!("Token {}", token);
        
        let influx_client = Client::new(server.url(), String::from(token));

        assert_eq!(influx_client.write_url, format!("{}/api/v2/write", server.url()), "influx write_url doesn't have the expected format when Client is created");
        assert_eq!(influx_client.token_header, token_header, "influx token header doesn't have the expected format when Client is created");

        for tested_line in get_tested_lines() {
            let mock = server.mock("POST", "/api/v2/write")
                .match_query(Matcher::AllOf(vec![
                    Matcher::UrlEncoded("org".into(), "someorg".into()),
                    Matcher::UrlEncoded("bucket".into(), "somebucket".into()),
                    Matcher::UrlEncoded("precision".into(), "ns".into()),
                ]))
                .match_header("authorization", token_header.as_str())
                .match_header("accept", "application/json")
                .match_header("Content-Type", "text/plain; charset=utf-8")
                .match_body(tested_line.expected_str)
                .with_status(204)
                .create_async().await;

            let _ = influx_client.write("someorg", "somebucket", tested_line.line).await;
            mock.assert();
        }

        let mock = server.mock("POST", "/api/v2/write")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("org".into(), "someorg".into()),
                Matcher::UrlEncoded("bucket".into(), "somebucket".into()),
                Matcher::UrlEncoded("precision".into(), "ns".into()),
            ]))
            .match_header("authorization", token_header.as_str())
            .match_header("accept", "application/json")
            .match_header("Content-Type", "text/plain; charset=utf-8")
            .match_body("")
            .with_status(204)
            .create_async().await;

        let _ = influx_client.test_write("someorg", "somebucket").await;
        mock.assert();
    }

    #[test]
    fn test_with_capacity() {
        let capacity = 100;
        let builder = LineProtocolBuilder::with_capacity(capacity);

        // Check that the buffer has the requested capacity
        assert!(builder.buf.capacity() >= capacity, "Buffer capacity is less than requested");

        // Ensure `after_first_field` is initialized correctly
        assert_eq!(builder.after_first_field, false, "after_first_field should be false on initialization");
    }

    //use alumet::plugin::PluginMetadata;
    //use alumet::agent::plugin::PluginInfo;
    //use alumet::{
    //    agent::{self, plugin::PluginSet},
    //    pipeline::{
    //        naming::{OutputName},
    //    },
    //    test::{StartupExpectations,RuntimeExpectations, runtime::OutputCheckInputContext},
    //    measurement::{MeasurementBuffer, WrappedMeasurementType},
    //    metrics::Metric,
    //    units::{Unit, PrefixedUnit},
    //};
    //use crate::{
    //    InfluxDbPlugin,
    //    Config,
    //    AttributeAs,
    //};
    //use httpmock::prelude::*;
    //use once_cell::sync::Lazy;
    //use plugin_tests::TestsPlugin;

    //#[test]
    //fn startup_ok() {
    //    let config = Config{
    //        host: String::from("http://localhost:8086"),
    //        token: String::from("seed-token"),
    //        org: String::from("seed"),
    //        bucket: String::from("pods"),
    //        attributes_as: AttributeAs::Field,
    //        attributes_as_tags: None,
    //        attributes_as_fields: None,
    //    };
    //    
    //    let mut plugins = PluginSet::new();
    //    plugins.add_plugin(PluginInfo{
    //        metadata: PluginMetadata::from_static::<InfluxDbPlugin>(),
    //        enabled: true,
    //        config: Some(config_to_toml_table(&config)),
    //    });
    //    let testConfig = toml::Value::try_from(plugin_tests::Config::default()).unwrap().as_table().unwrap().clone();
    //    PluginMetadata::from_static::<TestsPlugin>();

    //    let make_input = |ctx: &mut OutputCheckInputContext| -> MeasurementBuffer {
    //        println!("################");
    //        println!("INTO MAKE INPUT");
    //        println!("################");
    //        println!("{}", ctx.metrics().len());
    //        MeasurementBuffer::new()
    //    };
    //    let check_output = || {
    //        println!("################");
    //        println!("INTO CHECK OUTPUT");
    //        println!("################");
    //    };

    //    let runtime_expectations = RuntimeExpectations::new()
    //        .test_output(
    //            OutputName::from_str("influxdb", "out"),
    //            make_input,
    //            check_output,
    //        );
    //
    //    let agent = agent::Builder::new(plugins)
    //        .with_expectations(runtime_expectations)
    //        .build_and_start()
    //        .unwrap();
    //
    //    agent.pipeline.control_handle().shutdown();
    //    agent.wait_for_shutdown(Duration::from_secs(2)).unwrap();
    //}
    //fn config_to_toml_table(config: &Config) -> toml::Table {
    //    toml::Value::try_from(config).unwrap().as_table().unwrap().clone()
    //}
}
        //static SERVER: Lazy<MockServer> = Lazy::new(|| MockServer::start());
        //let test_write_mock = SERVER.mock(|when, then| {
        //    when.method(POST)
        //        .path("/api/v2/write")
        //        .query_param("org", "someorg")
        //        .query_param("bucket", "somebucket")
        //        .query_param("precision", "ns")
        //        .header("authorization", "Token sometoken")
        //        .header("Content-Type", "text/plain; charset=utf-8");
        //    then.status(204);
        //});

        //let another_mock = SERVER.mock(|when, then| {
        //    when.method(POST)
        //        .path("/testOK");
        //    then.status(200);
        //});

        //let config = Config{
        //    host: SERVER.url(""),
        //    token: String::from("sometoken"),
        //    org: String::from("someorg"),
        //    bucket: String::from("somebucket"),
        //    attributes_as: AttributeAs::Field,
        //    attributes_as_tags: None,
        //    attributes_as_fields: None,
        //};
            //ctx.metrics().register(Metric{
            //    name: String::from("some_metric"),
            //    description: String::from("some metric"),
            //    value_type: WrappedMeasurementType::U64,
            //    unit: PrefixedUnit::nano(Unit::Second),

            //});
            //println!("Generating MeasurementBuffer...");
            //let buffer = MeasurementBuffer::new();
            //let my_metric alumet.create_metric(
            //    "some_metric",
            //    Unit::Unity,
            //    "some dumb metric",
            //)?,
            //buffer.push(new_point("2025-02-10T13:19:00Z", WrappedMeasurementValue::U64(0), 0));
            //test_write_mock.assert();
            //another_mock.assert();
        //plugins.add_plugin(PluginInfo{
        //    metadata: PluginMetadata::from_static::<TestsPlugin>(),
        //    enabled: true,
        //    config: Some(testConfig),
        //});
        //let startup_expectations = StartupExpectations::new()
        //    .expect_output("influxdbd", "oute");

