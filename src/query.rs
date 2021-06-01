use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    lines: Vec<String>,
}

impl Query {
    pub fn new(line: impl Into<String>) -> Self {
        let lines = vec![line.into()];
        Self { lines }
    }

    /// Create a query from a raw string.
    ///
    /// ## Example
    /// ```rust
    /// # use influx::Query;
    /// let query = Query::raw(r#"from(bucket: "server")
    ///     |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    ///     |> filter(fn: (r) => r["_measurement"] == "example_measurement")
    ///     |> keys()"#);
    /// ```
    pub fn raw(query: impl Into<String>) -> Self {
        let lines = query
            .into()
            .lines()
            .map(|l| match l.strip_prefix("|>") {
                Some(stripped) => stripped.trim().to_owned(),
                None => l.trim().to_owned(),
            })
            .collect();
        Self { lines }
    }

    /// Append a line to the query.
    ///
    /// ## Example
    /// ```rust
    /// # use influx::Query;
    /// let query = Query::new(r#"from(bucket: "example_bucket")"#)
    ///     .then(r#"filter(fn: (r) => r["_measurement"] == "example_measurement")"#);
    /// ```
    pub fn then(mut self, line: impl Into<String>) -> Self {
        self.lines.push(line.into());
        self
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.lines
                .iter()
                .map(|l| l.to_string())
                .collect::<Vec<_>>()
                .join("\n |> ")
        )
    }
}
