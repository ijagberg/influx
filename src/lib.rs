pub use client::{InfluxClient, InfluxClientBuilder, InfluxClientBuilderError, InfluxError};
pub use query::Query;
use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    time::{SystemTime, SystemTimeError},
};

mod client;
mod query;

#[macro_use]
extern crate log;

#[derive(Debug, Clone, PartialEq)]
struct TagValue(String);

impl TagValue {
    fn new(s: String) -> Self {
        let s = s
            .replace(',', "\\,")
            .replace('=', "\\=")
            .replace(' ', "\\ ");
        Self(s)
    }
}

/// Represents various supported field values.
///
/// Fields can be floats, strings, bools, signed and unsigned integers.
#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    /// A float field
    Float(f64),
    /// A string field
    String(String),
    /// A bool field
    Bool(bool),
    /// An integer field
    Integer(i128),
    /// An unsigned integer field
    UInteger(u128),
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Field::Float(v) => write!(f, "{}", v),
            Field::String(v) => write!(f, r#""{}""#, v),
            Field::Bool(v) => write!(f, "{}", v),
            Field::Integer(v) => write!(f, "{}i", v),
            Field::UInteger(v) => write!(f, "{}u", v),
        }
    }
}

macro_rules! impl_uint {
    ($from_type:ty) => {
        impl From<$from_type> for Field {
            fn from(v: $from_type) -> Self {
                Field::UInteger(v as u128)
            }
        }
    };
}

impl_uint!(u8);
impl_uint!(u16);
impl_uint!(u32);
impl_uint!(u64);
impl_uint!(u128);

macro_rules! impl_int {
    ($from_type:ty) => {
        impl From<$from_type> for Field {
            fn from(v: $from_type) -> Self {
                Field::Integer(v as i128)
            }
        }
    };
}

impl_int!(i8);
impl_int!(i16);
impl_int!(i32);
impl_int!(i64);
impl_int!(i128);

macro_rules! impl_float {
    ($from_type:ty) => {
        impl From<$from_type> for Field {
            fn from(v: $from_type) -> Self {
                Field::Float(v as f64)
            }
        }
    };
}

impl_float!(f32);
impl_float!(f64);

impl From<bool> for Field {
    fn from(v: bool) -> Self {
        Field::Bool(v)
    }
}

impl From<String> for Field {
    fn from(v: String) -> Self {
        Field::String(v)
    }
}

impl From<&str> for Field {
    fn from(v: &str) -> Self {
        Field::String(v.into())
    }
}

/// Represents a point of measurement in Influx
///
/// ## Example
/// To create a measurement, you can either call `new` directly, or use the builder method:
/// ```rust
/// # use influxrs::*;
/// let measurement = Measurement::builder("gps")
///     .field("latitude", 40.447992135544304)
///     .field("longitude", -3.689346313476562)
///     .tag("country", "Spain")
///     .tag("city", "Madrid")
///     .timestamp_ms(1622888382963) //if no timestamp is specified, the current time is used
///     .build()
///     .unwrap(); // building can fail if no fields are specified
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Measurement {
    /// Name of measurement
    measurement_name: String,
    /// Timestamp of measurement as a Unix Epoch (ms)
    timestamp_ms: u128,
    /// Tags of measurement
    tags: HashMap<String, TagValue>,
    /// Fields of measurement
    fields: HashMap<String, Field>,
}

impl Measurement {
    fn new(
        measurement_name: String,
        timestamp_ms: u128,
        tags: HashMap<String, TagValue>,
        fields: HashMap<String, Field>,
    ) -> Self {
        Self {
            measurement_name,
            timestamp_ms,
            tags,
            fields,
        }
    }

    pub fn builder(measurement_name: impl Into<String>) -> MeasurementBuilder {
        MeasurementBuilder::new(measurement_name)
    }

    /// Add a field to the measurement.
    pub fn add_field(&mut self, name: impl Into<String>, value: impl Into<Field>) {
        self.fields.insert(name.into(), value.into());
    }

    /// Add a tag to the measurement.
    pub fn add_tag(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.tags.insert(name.into(), TagValue::new(value.into()));
    }

    fn measurement_part(&self) -> &str {
        &self.measurement_name
    }

    fn tags_part(&self) -> String {
        self.tags
            .iter()
            .map(|(name, value)| format!("{}={}", name, value.0))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fields_part(&self) -> String {
        self.fields
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Convert this `Measurement` to Influx line protocol.
    pub fn to_line_protocol(&self) -> String {
        if self.tags.is_empty() {
            format!(
                "{} {} {}",
                self.measurement_part(),
                self.fields_part(),
                self.timestamp_ms
            )
        } else {
            format!(
                "{},{} {} {}",
                self.measurement_part(),
                self.tags_part(),
                self.fields_part(),
                self.timestamp_ms
            )
        }
    }
}

pub struct MeasurementBuilder {
    name: String,
    tags: Vec<(String, TagValue)>,
    fields: Vec<(String, Field)>,
    timestamp: Option<u128>,
}

impl MeasurementBuilder {
    fn new(measurement_name: impl Into<String>) -> Self {
        MeasurementBuilder {
            name: measurement_name.into(),
            tags: Vec::new(),
            fields: Vec::new(),
            timestamp: None,
        }
    }

    pub fn tag(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push((name.into(), TagValue::new(value.into())));
        self
    }

    pub fn field(mut self, name: impl Into<String>, value: impl Into<Field>) -> Self {
        self.fields.push((name.into(), value.into()));
        self
    }

    pub fn timestamp_ms(mut self, timestamp_ms: u128) -> Self {
        self.timestamp = Some(timestamp_ms);
        self
    }

    pub fn build(self) -> Result<Measurement, MeasurementBuilderError> {
        if self.fields.is_empty() {
            Err(MeasurementBuilderError::EmptyFields)
        } else {
            let timestamp_ms = if let Some(timestamp_ms) = self.timestamp {
                timestamp_ms
            } else {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_millis()
            };
            Ok(Measurement::new(
                self.name,
                timestamp_ms,
                self.tags.into_iter().collect(),
                self.fields.into_iter().collect(),
            ))
        }
    }
}

#[derive(Debug)]
pub enum MeasurementBuilderError {
    EmptyFields,
    SystemTimeError(SystemTimeError),
}

impl Display for MeasurementBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = match self {
            MeasurementBuilderError::EmptyFields => "fields cannot be empty".to_string(),
            MeasurementBuilderError::SystemTimeError(e) => format!("SystemTimeError: '{}'", e),
        };

        write!(f, "{}", output)
    }
}

impl From<SystemTimeError> for MeasurementBuilderError {
    fn from(e: SystemTimeError) -> Self {
        Self::SystemTimeError(e)
    }
}

impl Error for MeasurementBuilderError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_from() {
        assert_eq!(Field::from(1_i8), Field::Integer(1));
        assert_eq!(Field::from(2_i16), Field::Integer(2));
        assert_eq!(Field::from(3_i32), Field::Integer(3));
        assert_eq!(Field::from(4_i64), Field::Integer(4));
        assert_eq!(Field::from(5_i128), Field::Integer(5));

        assert_eq!(Field::from(1_u8), Field::UInteger(1));
        assert_eq!(Field::from(2_u16), Field::UInteger(2));
        assert_eq!(Field::from(3_u32), Field::UInteger(3));
        assert_eq!(Field::from(4_u64), Field::UInteger(4));
        assert_eq!(Field::from(5_u128), Field::UInteger(5));

        assert_eq!(Field::from(1.5_f32), Field::Float(1.5));
        assert_eq!(Field::from(2.5_f64), Field::Float(2.5));

        assert_eq!(Field::from(true), Field::Bool(true));
        assert_eq!(Field::from(false), Field::Bool(false));

        assert_eq!(Field::from("s".to_string()), Field::String("s".to_string()));
    }

    #[test]
    fn measurement() {
        let m = Measurement::builder("example_measurement")
            .tag("tag_1", "tag_value_1")
            .tag("tag_2", "tag_value_2")
            .field("bool_field", true)
            .field("uinteger_field", 100_u16)
            .field("integer_field", -100)
            .field("float_field", 10.123)
            .field("string_field", "string_value")
            .timestamp_ms(1602321877560)
            .build()
            .unwrap();

        println!("{:?}", m);

        assert_eq!(
            m,
            Measurement {
                measurement_name: "example_measurement".to_string(),
                tags: vec![("tag_1", "tag_value_1"), ("tag_2", "tag_value_2")]
                    .into_iter()
                    .map(|(name, value)| (name.to_string(), TagValue::new(value.to_string())))
                    .collect(),
                fields: vec![
                    ("bool_field", Field::Bool(true)),
                    ("uinteger_field", Field::UInteger(100)),
                    ("integer_field", Field::Integer(-100)),
                    ("float_field", Field::Float(10.123)),
                    ("string_field", Field::String("string_value".to_string()))
                ]
                .into_iter()
                .map(|(name, value)| (name.to_string(), value))
                .collect(),
                timestamp_ms: 1602321877560
            }
        );
    }

    #[test]
    fn measurement_escaping() {
        let m = Measurement::builder("example_measurement")
            .tag("agent", "KHTML, like Gecko")
            .field("count", 1.0)
            .timestamp_ms(1602321877560)
            .build()
            .unwrap();

        assert_eq!(
            m.to_line_protocol(),
            r#"example_measurement,agent=KHTML\,\ like\ Gecko count=1 1602321877560"#,
        );
    }
}
