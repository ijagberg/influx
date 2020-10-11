use influx::{
    query::Function, query::GroupMode, query::OnEmpty, query::Query, InfluxClient, Measurement,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("{:?}", args);
    let client = InfluxClient::builder(args[1].clone(), args[2].clone(), String::from("ijagberg"))
        .build()
        .unwrap();

    let response = client.send_query(example_query()).await;
    println!("{:#?}", response.status());
    let body = response.text().await.unwrap();
    let mut reader = csv::Reader::from_reader(body.as_bytes());
    for record in reader.records() {
        println!("{:?}", record.unwrap());
    }
}

fn example_query() -> Query {
    let lines = vec![
        Function::From {
            bucket: "server".into(),
        },
        Function::Range {
            start: 1602406555,
            stop: 1602406655,
        },
        Function::Filter {
            function: r#"(r) => r["_measurement"] == "handle_request""#.into(),
            on_empty: OnEmpty::Drop,
        },
    ];
    let query = Query::new(lines.iter().map(|l| l.to_string()).collect());
    query
}
