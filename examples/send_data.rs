use chrono::{Duration, Utc};
use influx::{InfluxClient, Measurement, Query};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    pretty_env_logger::init();

    let (address, key, org, bucket) = (
        std::env::var("INFLUX_ADDRESS").unwrap(),
        std::env::var("INFLUX_KEY").unwrap(),
        std::env::var("INFLUX_ORG").unwrap(),
        std::env::var("INFLUX_BUCKET").unwrap(),
    );

    let client = InfluxClient::builder(address, key, org).build().unwrap();

    let response = client
        .write(&bucket, &get_example_measurements())
        .await
        .unwrap();
    if response.status().is_success() {
        let response = client
            .query(
                Query::new(format!(r#"from(bucket: "{}")"#, bucket))
                    .then(format!(
                        r#"range(start: {}, stop: {})"#,
                        five_minutes_ago(),
                        five_minutes_from_now()
                    ))
                    .then(r#"filter(fn: (r) => r["_measurement"] == "m1")"#),
            )
            .await
            .unwrap();

        println!("{:#?}", response);
    }
}

fn get_example_measurements() -> Vec<Measurement> {
    let m1 = Measurement::builder("m1")
        .tag("tag1", "tag1_value")
        .tag("tag2", "tag2_value")
        .field("field1", "string_value")
        .field("field2", true)
        .build()
        .unwrap();

    vec![m1]
}

fn five_minutes_ago() -> i64 {
    (Utc::now() - Duration::minutes(5)).timestamp()
}

fn five_minutes_from_now() -> i64 {
    (Utc::now() + Duration::minutes(5)).timestamp()
}
