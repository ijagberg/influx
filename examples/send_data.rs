use influx::{InfluxClient, Measurement};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("{:?}", args);
    let client = InfluxClient::builder(args[1].clone(), args[2].clone(), String::from(""))
        .build()
        .unwrap();

    let response = client
        .send_batch("server", &get_example_measurements())
        .await;
    println!("{:#?}", response.status());
    let body = response.text().await.unwrap();
    println!("{:#?}", body);
}

fn get_example_measurements() -> Vec<Measurement> {
    let m1 = Measurement::builder(String::from("m1"))
        .with_tag(String::from("tag1"), String::from("tag1_value"))
        .with_tag(String::from("tag2"), String::from("tag2_value"))
        .with_field_string(String::from("field1"), String::from("string_value"))
        .with_field_bool(String::from("field2"), true)
        .build()
        .unwrap();

    vec![m1]
}
