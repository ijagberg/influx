use std::fmt::Debug;

pub use client::InfluxClient;

pub mod client;
pub mod query;

use std::{collections::HashMap, fmt::Display, time::SystemTime, time::SystemTimeError};

/// Represents various supported field values
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

/// Represents a point of measurement in Influx
pub struct Measurement {
    /// Name of measurement
    measurement_name: String,
    /// Timestamp of measurement as a Unix Epoch (ms)
    timestamp_ms: u128,
    /// Tags of measurement
    tags: HashMap<String, String>,
    /// Fields of measurement
    fields: HashMap<String, Field>,
}

impl Measurement {
    pub fn builder(measurement_name: String) -> MeasurementBuilder {
        MeasurementBuilder::new(measurement_name)
    }

    fn measurement_part(&self) -> &str {
        &self.measurement_name
    }

    fn tags_part(&self) -> String {
        self.tags
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fields_part(&self) -> String {
        self.fields
            .iter()
            .map(|(name, value)| format!("{}={}", name, value.to_string()))
            .collect::<Vec<_>>()
            .join(",")
    }

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
    tags: Vec<(String, String)>,
    fields: Vec<(String, Field)>,
    timestamp: Option<u128>,
}

impl MeasurementBuilder {
    fn new(measurement_name: String) -> Self {
        MeasurementBuilder {
            name: measurement_name,
            tags: Vec::new(),
            fields: Vec::new(),
            timestamp: None,
        }
    }

    pub fn with_tag(mut self, name: String, value: String) -> Self {
        self.tags.push((name, value));
        self
    }

    pub fn with_field(mut self, name: String, value: Field) -> Self {
        self.fields.push((name, value));
        self
    }

    pub fn with_field_f64(mut self, name: String, value: f64) -> Self {
        self.fields.push((name, Field::Float(value)));
        self
    }

    pub fn with_field_string(mut self, name: String, value: String) -> Self {
        self.fields.push((name, Field::String(value)));
        self
    }

    pub fn with_field_bool(mut self, name: String, value: bool) -> Self {
        self.fields.push((name, Field::Bool(value)));
        self
    }

    pub fn with_field_u128(mut self, name: String, value: u128) -> Self {
        self.fields.push((name, Field::UInteger(value)));
        self
    }

    pub fn with_field_i128(mut self, name: String, value: i128) -> Self {
        self.fields.push((name, Field::Integer(value)));
        self
    }

    pub fn with_timestamp(mut self, timestamp: u128) -> Self {
        self.timestamp = Some(timestamp);
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
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| MeasurementBuilderError::TimestampError(e))?
                    .as_millis()
            };
            Ok(Measurement {
                measurement_name: self.name,
                fields: self.fields.into_iter().collect(),
                tags: self.tags.into_iter().collect(),
                timestamp_ms,
            })
        }
    }
}

#[derive(Debug)]
pub enum MeasurementBuilderError {
    EmptyFields,
    TimestampError(SystemTimeError),
}

impl Display for MeasurementBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeasurementBuilderError::EmptyFields => write!(f, "fields cannot be empty"),
            MeasurementBuilderError::TimestampError(err) => {
                write!(f, "error evaluating timestamp: '{}'", err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement() {
        Measurement::builder(String::from("example_measurement"))
            .with_tag(String::from("tag_1"), String::from("tag_value_1"))
            .with_tag(String::from("tag_2"), String::from("tag_value_2"))
            .with_field(String::from("field_1"), Field::Bool(true))
            .with_field(String::from("field_2"), Field::Integer(100))
            .with_field(String::from("field_3"), Field::Float(10.123))
            .with_field(
                String::from("field_4"),
                Field::String(String::from("string_value")),
            )
            .with_timestamp(1602321877560)
            .build()
            .unwrap();
    }
}
