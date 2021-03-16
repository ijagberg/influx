use influx::{InfluxClient, Measurement};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("{:?}", args);
    let client = InfluxClient::builder(args[1].clone(), args[2].clone(), String::from("ijagberg"))
        .build()
        .unwrap();

    let response = client
        .send_batch("server", &get_example_measurements())
        .await
        .unwrap();
    println!("{:#?}", response.status());
    let body = response.text().await.unwrap();
    println!("{:#?}", body);
}

fn get_example_measurements() -> Vec<Measurement> {
    let m1 = Measurement::builder("m1")
        .with_tag("tag1", "tag1_value")
        .with_tag("tag2", "tag2_value")
        .with_field("field1", "string_value")
        .with_field("field2", true)
        .build()
        .unwrap();

    vec![m1]
}
