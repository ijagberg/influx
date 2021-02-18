use influx::{
    query::{OnEmpty, Query},
    InfluxClient,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("{:?}", args);
    let client = InfluxClient::builder(args[1].clone(), args[2].clone(), String::from("ijagberg"))
        .build()
        .unwrap();

    let response = client.send_query(example_query()).await.unwrap();
    println!("{:#?}", response.status());
    let body = response.text().await.unwrap();
    let mut reader = csv::Reader::from_reader(body.as_bytes());
    for record in reader.deserialize() {
        let row: Row = record.unwrap();
        println!("{:?}", row);
    }
}

fn example_query() -> Query {
    Query::new()
        .from("server".into())
        .range(1613052000, 1613052600)
        .filter(
            r#"(r) => r["_measurement"] == "handle_request""#.into(),
            OnEmpty::Drop,
        )
        .filter(
            r#"(r) => r["_field"] == "duration_micros""#.into(),
            OnEmpty::Drop,
        )
        .filter(
            r#"(r) => r["method"] == "add_prediction""#.into(),
            OnEmpty::Drop,
        )
        .r#yield("mean".into())
}

#[derive(Debug, serde::Deserialize)]
struct Row {
    name: String,
    id: String,
    #[serde(alias = "organizationID")]
    organization_id: String,
    #[serde(alias = "retentionPolicy")]
    retention_policy: Option<String>,
    #[serde(alias = "retentionPeriod")]
    retention_period: u128,
}
