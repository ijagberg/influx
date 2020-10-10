use std::{collections::HashMap, time::SystemTime};

use influx_contracts::{Field, Measurement};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("{:?}", args);
    let client = influx_client::InfluxClient::builder(
        args[1].clone(),
        args[2].clone(),
        String::from(""),
    )
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
    let mut tags = HashMap::new();
    tags.insert(String::from("tag1"), String::from("tag1_value"));
    tags.insert(String::from("tag2"), String::from("tag2_value"));

    let mut fields = HashMap::new();
    //fields.insert(String::from("field1"), Field::Bool(true));
    fields.insert(
        String::from("field2"),
        Field::String(String::from("string_value")),
    );

    let m1 = Measurement::new(
        String::from("measurement1"),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        tags,
        fields,
    );

    vec![m1]
}
