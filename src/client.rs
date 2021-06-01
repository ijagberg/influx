use crate::{query::Query, Measurement};
use reqwest::Method;
use std::{collections::HashMap, error::Error, fmt::Display};

pub use reqwest::Response;

pub struct InfluxClient {
    url: String,
    key: String,
    org: String,
    http_client: reqwest::Client,
}

impl InfluxClient {
    fn new(url: String, key: String, org: String, http_client: reqwest::Client) -> Self {
        Self {
            url,
            key,
            org,
            http_client,
        }
    }

    pub fn builder(url: String, key: String, org: String) -> InfluxClientBuilder {
        InfluxClientBuilder::new(url, key, org)
    }

    pub async fn write(
        &self,
        bucket: &str,
        measurements: &[Measurement],
    ) -> Result<Response, InfluxError> {
        let payload = measurements
            .iter()
            .map(|m| m.to_line_protocol())
            .collect::<Vec<_>>()
            .join("\n");
        let url = format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ms",
            self.url, self.org, bucket
        );
        info!("posting payload to influx at '{}': '{}'", url, payload);
        Ok(self
            .http_client
            .post(&url)
            .header("Authorization", format!("Token {}", &self.key))
            .body(payload)
            .send()
            .await?)
    }

    pub async fn query(&self, query: Query) -> Result<Vec<HashMap<String, String>>, InfluxError> {
        let payload = query.to_string();

        let url = format!("{}/api/v2/query?org={}", self.url, self.org);
        debug!("posting query to influx at '{}': '{}'", url, payload);

        let request = self
            .http_client
            .request(Method::POST, &url)
            .header("Authorization", format!("Token {}", &self.key))
            .header("Content-type", "application/vnd.flux")
            .header("Accept", "application/csv")
            .body(payload)
            .build()?;

        let response = self.http_client.execute(request).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.ok();
            return Err(InfluxError::NonSuccessResponse(status, body));
        }

        let body = response.text().await?;

        trace!("response body: '{}'", body);

        let lines: Vec<String> = body.lines().map(|l| l.trim().to_owned()).collect();
        let tables: Vec<_> = lines
            .split(|t| t.is_empty())
            .filter(|t| !t.is_empty())
            .map(|t| t.join("\n"))
            .collect();

        let mut records = Vec::new();
        for table in tables {
            let mut reader = csv::Reader::from_reader(table.as_bytes());
            for result in reader.deserialize() {
                let mut record: HashMap<String, String> = result?;
                record.remove("");
                records.push(record);
            }
        }

        Ok(records)
    }
}

pub struct InfluxClientBuilder {
    url: String,
    key: String,
    org: String,
}

impl InfluxClientBuilder {
    fn new(url: String, key: String, org: String) -> Self {
        Self { url, key, org }
    }

    pub fn build(self) -> Result<InfluxClient, InfluxClientBuilderError> {
        Ok(InfluxClient::new(
            self.url,
            self.key,
            self.org,
            reqwest::Client::new(),
        ))
    }
}

#[derive(Debug)]
pub enum InfluxClientBuilderError {}

#[derive(Debug)]
pub enum InfluxError {
    ReqwestError(reqwest::Error),
    CsvError(csv::Error),
    NonSuccessResponse(reqwest::StatusCode, Option<String>),
}

impl Error for InfluxError {}

impl From<reqwest::Error> for InfluxError {
    fn from(err: reqwest::Error) -> Self {
        Self::ReqwestError(err)
    }
}

impl From<csv::Error> for InfluxError {
    fn from(err: csv::Error) -> Self {
        Self::CsvError(err)
    }
}

impl Display for InfluxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = match self {
            InfluxError::ReqwestError(e) => {
                format!("reqwest error: '{}'", e)
            }
            InfluxError::NonSuccessResponse(status, body) => match body {
                Some(body) => format!("non-success response: '{}', body: '{}'", status, body),
                None => format!("non-success response: '{}'", status),
            },
            InfluxError::CsvError(e) => format!("csv error: '{}'", e),
        };

        write!(f, "{}", output)
    }
}
