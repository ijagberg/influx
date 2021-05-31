use influx::{InfluxClient, Measurement};

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
        .send_batch(&bucket, &get_example_measurements())
        .await
        .unwrap();
    let body = response.text().await.unwrap();
    println!("{:#?}", body);
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
