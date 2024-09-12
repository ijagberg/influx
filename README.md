# influxrs

This crate contains some useful structs for publishing data to, and reading data from InfluxDB.

## Model

The `Measurement` struct represents a single measurement in Influx. Recommended way to create a `Measurement` is to use the `Measurement::builder` function.

```rust
let measurement = Measurement::builder("m1")
    .tag("tag1", "tag1_value")
    .tag("tag2", "tag2_value")
    .field("field1", "string_value")
    .field("field2", true)
    .timestamp_ms(1622493622000) // milliseconds since the Unix epoch
    .build()
    .unwrap();
// convert it to InfluxDB line protocol
let line = measurement.to_line_protocol();
```

## Client

Enable the `client` feature to gain access to a very simple client struct that simplifies writing and reading data from a specified InfluxDB instance. This client is very rudimentary, and it might be better to just use a regular HTTP client instead.

### Creating a client

```rust
let client = InfluxClient::builder("www.example.com", "example-key", "example-org").build().unwrap();
```

### Writing data

```rust
let measurement =
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

When querying data, a `Vec<HashMap<String, String>>` is returned, containing individual csv records:

```json
{
  "result": "_result",
  "_value": "string_value",
  "table": "0",
  "_start": "2021-06-01T11:13:15Z",
  "_field": "field1",
  "tag1": "tag1_value",
  "tag2": "tag2_value",
  "_time": "2021-06-01T11:16:05.684Z",
  "_measurement": "m1",
  "_stop": "2021-06-01T11:23:15Z"
}
```
