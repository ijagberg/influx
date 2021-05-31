## Creating a client
```rust
let client = InfluxClient::builder("www.example.com", "example-key", "example-org").build().unwrap();
```

## Writing data
```rust
let measurement = Measurement::builder("m1")
    .tag("tag1", "tag1_value")
    .tag("tag2", "tag2_value")
    .field("field1", "string_value")
    .field("field2", true)
    .timestamp(1622493622)
    .build()
    .unwrap();
let response = client
    .write("example-bucket", &[measurement]) // can post a batch if we want
    .await
    .unwrap();
```

## Querying data
```rust
let response = client
    .query(
        Query::new(r#"from(bucket: "example-bucket")"#)
            .then(r#"range(start: 1622493322, stop: 1622493922)"#)
            .then(r#"filter(fn: (r) => r["_measurement"] == "m1")"#),
    )
    .await
    .unwrap();
```