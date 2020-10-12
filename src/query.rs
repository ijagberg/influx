use std::fmt::Display;

fn comma_join_strings<T>(v: &[T]) -> String
where
    T: Display,
{
    v.iter()
        .map(|c| format!(r#""{}""#, c))
        .collect::<Vec<_>>()
        .join(", ")
}

fn comma_join<T>(v: &[T]) -> String
where
    T: Display,
{
    v.iter()
        .map(|c| format!(r#"{}"#, c))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    lines: Vec<String>,
}

impl Query {
    pub fn new() -> Self {
        Self { lines: Vec::new() }
    }

    pub fn with(mut self, function: Function) -> Self {
        self.lines.push(function.to_string());
        self
    }

    pub fn with_text(mut self, text: String) -> Self {
        self.lines.push(text);
        self
    }

    pub fn from(mut self, bucket: String) -> Self {
        self.lines.push(Function::From { bucket }.to_string());
        self
    }

    pub fn range(mut self, start: u128, stop: u128) -> Self {
        self.lines.push(Function::Range { start, stop }.to_string());
        self
    }

    pub fn filter(mut self, function: String, on_empty: OnEmpty) -> Self {
        self.lines
            .push(Function::Filter { function, on_empty }.to_string());
        self
    }

    pub fn group(mut self, columns: Vec<String>, mode: GroupMode) -> Self {
        self.lines
            .push(Function::Group { columns, mode }.to_string());
        self
    }

    pub fn r#yield(mut self, name: String) -> Self {
        self.lines.push(Function::Yield { name }.to_string());
        self
    }

    pub fn keep(mut self, columns: Vec<String>, function: String) -> Self {
        self.lines
            .push(Function::Keep { columns, function }.to_string());
        self
    }

    pub fn drop(mut self, columns: Vec<String>, function: String) -> Self {
        self.lines
            .push(Function::Drop { columns, function }.to_string());
        self
    }

    pub fn tail(mut self, n: u32, offset: u32) -> Self {
        self.lines.push(Function::Tail { n, offset }.to_string());
        self
    }

    pub fn contains<T>(mut self, value: T, set: Vec<T>) -> Self
    where
        TypeValue: From<T>,
    {
        self.lines.push(
            Function::Contains {
                value: value.into(),
                set: set.into_iter().map(|v| TypeValue::from(v)).collect(),
            }
            .to_string(),
        );
        self
    }

    pub fn distinct(mut self, column: String) -> Self {
        self.lines.push(Function::Distinct { column }.to_string());
        self
    }

    pub fn max(mut self) -> Self {
        self.lines.push(Function::Max.to_string());
        self
    }

    pub fn min(mut self) -> Self {
        self.lines.push(Function::Min.to_string());
        self
    }

    pub fn limit(mut self, n: u32, offset: u32) -> Self {
        self.lines.push(Function::Limit { n, offset }.to_string());
        self
    }

    pub fn set(mut self, key: String, value: String) -> Self {
        self.lines.push(Function::Set { key, value }.to_string());
        self
    }

    pub fn sort(mut self, columns: Vec<String>, desc: bool) -> Self {
        self.lines
            .push(Function::Sort { columns, desc }.to_string());
        self
    }

    pub fn count(mut self, column: String) -> Self {
        self.lines.push(Function::Count { column }.to_string());
        self
    }

    pub fn buckets(mut self) -> Self {
        self.lines.push(Function::Buckets.to_string());
        self
    }

    pub fn integral(mut self, unit: String, column: String, time_column: String) -> Self {
        self.lines.push(
            Function::Integral {
                unit,
                column,
                time_column,
            }
            .to_string(),
        );
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

pub enum Function {
    /// Used to retrieve data from an InfluxDB data source.
    /// It returns a stream of tables from the specified bucket.
    /// Each unique series is contained within its own table.
    /// Each record in the table represents a single point in the series.
    From { bucket: String },
    /// Filters records based on time bounds.
    /// Each input table's records are filtered to contain only records that exist within the time bounds.
    /// Each input table's group key value is modified to fit within the time bounds.
    /// Tables where all records exists outside the time bounds are filtered entirely.
    Range { start: u128, stop: u128 },
    /// Filters data based on conditions defined in the function.
    /// The output tables have the same schema as the corresponding input tables.
    Filter { function: String, on_empty: OnEmpty },
    /// Groups records based on their values for specific columns.
    /// It produces tables with new group keys based on provided properties.
    Group {
        columns: Vec<String>,
        mode: GroupMode,
    },
    /// Indicates the input tables received should be delivered as a result of the query.
    /// Yield outputs the input stream unmodified.
    /// A query may have multiple results, each identified by the name provided to the `yield()` function.
    Yield { name: String },
    /// Returns a table containing only the specified columns, ignoring all others.
    /// Only columns in the group key that are also specified in the `keep()` function will be kept in the resulting group key.
    /// It is the inverse of `drop`.
    Keep {
        columns: Vec<String>,
        function: String,
    },
    /// Removes specified columns from a table.
    /// Columns can be specified either through a list or a predicate function.
    /// When a dropped column is part of the group key, it will be removed from the key.
    Drop {
        columns: Vec<String>,
        function: String,
    },
    /// Limits each output table to the last `n` records, excluding the offset.
    Tail { n: u32, offset: u32 },
    /// Tests whether a value is a member of a set.
    Contains {
        value: TypeValue,
        set: Vec<TypeValue>,
    },
    /// Returns the unique values for a given column.
    Distinct {
        /// Column on which to track unique values.
        column: String,
    },
    /// Selects record with the highest `_value` from the input table.
    Max,
    /// Selects record with the lowest `_value` from the input table.
    Min,
    /// Limits each output table to the first `n` records, excluding the offset.
    Limit { n: u32, offset: u32 },
    /// Assigns a static value to each record in the input table.
    /// The key may modify an existing column or add a new column to the tables.
    /// If the modified column is part of the group key, the output tables are regrouped as needed.
    Set { key: String, value: String },
    /// Orders the records within each table.
    /// One output table is produced for each input table.
    /// The output tables will have the same schema as their corresponding input tables.
    Sort { columns: Vec<String>, desc: bool },
    /// Outputs the number of records in the specified column.
    Count { column: String },
    /// Returns a list of buckets in the organization.
    Buckets,
    /// Computes the area under the curve per unit of time of subsequent non-null records.
    /// The curve is defined using `_time` as the domain and record values as the range.
    Integral {
        unit: String,
        column: String,
        time_column: String,
    },
}

pub enum TypeValue {
    Bool(bool),
    Integer(i64),
    UInteger(u64),
    Float(f64),
    String(String),
    Time(u128),
}

impl From<bool> for TypeValue {
    fn from(v: bool) -> Self {
        TypeValue::Bool(v)
    }
}

impl From<i64> for TypeValue {
    fn from(v: i64) -> Self {
        TypeValue::Integer(v)
    }
}

impl From<u64> for TypeValue {
    fn from(v: u64) -> Self {
        TypeValue::UInteger(v)
    }
}

impl From<f64> for TypeValue {
    fn from(v: f64) -> Self {
        TypeValue::Float(v)
    }
}

impl From<String> for TypeValue {
    fn from(v: String) -> Self {
        TypeValue::String(v)
    }
}

impl From<u128> for TypeValue {
    fn from(v: u128) -> Self {
        TypeValue::Time(v)
    }
}

impl Display for TypeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeValue::Bool(v) => write!(f, "{}", v),
            TypeValue::Integer(v) => write!(f, "{}", v),
            TypeValue::UInteger(v) => write!(f, "{}", v),
            TypeValue::Float(v) => write!(f, "{}", v),
            TypeValue::String(v) => write!(f, r#""{}""#, v),
            TypeValue::Time(v) => write!(f, "{}", v),
        }
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Function::From { bucket } => write!(f, r#"from(bucket: "{}")"#, bucket),
            Function::Range { start, stop } => {
                write!(f, r#"range(start: {}, stop: {})"#, start, stop)
            }
            Function::Filter { function, on_empty } => {
                write!(f, r#"filter(fn: {}, onEmpty: "{}")"#, function, on_empty)
            }
            Function::Group { columns, mode } => write!(
                f,
                r#"group(columns: [{}], mode:"{}")"#,
                comma_join_strings(columns),
                mode
            ),
            Function::Yield { name } => write!(f, r#"yield(name: "{}""#, name),
            Function::Keep { columns, function } => write!(
                f,
                r#"keep(columns: [{}], fn: {})"#,
                comma_join_strings(columns),
                function
            ),
            Function::Drop { columns, function } => write!(
                f,
                r#"drop(columns: [{}], fn: {})"#,
                comma_join_strings(columns),
                function
            ),
            Function::Tail { n, offset } => write!(f, r#"tail(n: {}, offset: {})"#, n, offset),
            Function::Contains { value, set } => write!(
                f,
                r#"contains(value: {}, set: [{}])"#,
                value,
                comma_join(set)
            ),
            Function::Distinct { column } => write!(f, r#"distinct(column: "{}")"#, column),
            Function::Max => write!(f, "max()"),
            Function::Min => write!(f, "min()"),
            Function::Limit { n, offset } => write!(f, r#"limit(n: {}, offset: {})"#, n, offset),
            Function::Set { key, value } => write!(f, r#"set(key: "{}", value: "{}")"#, key, value),
            Function::Sort { columns, desc } => write!(
                f,
                r#"sort(columns: [{}], desc: {}"#,
                comma_join_strings(columns),
                desc
            ),
            Function::Count { column } => write!(f, r#"count(column: "{}")"#, column),
            Function::Buckets => write!(f, r#"buckets()"#),
            Function::Integral {
                unit,
                column,
                time_column,
            } => write!(
                f,
                r#"integral(unit: {}, column: "{}", timeColumn: "{}")"#,
                unit, column, time_column
            ),
        }
    }
}

pub enum GroupMode {
    By,
    Except,
}

impl Display for GroupMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupMode::By => write!(f, "by"),
            GroupMode::Except => write!(f, "except"),
        }
    }
}

pub enum OnEmpty {
    Keep,
    Drop,
}

impl Display for OnEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnEmpty::Keep => write!(f, "keep"),
            OnEmpty::Drop => write!(f, "drop"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_query() {
        let query1 = Query::new()
            .with(Function::From {
                bucket: "server".into(),
            })
            .with(Function::Range {
                start: 1602404530510000000,
                stop: 1602404530610000000,
            })
            .with(Function::Filter {
                function: r#"(r) => r["_measurement"] == "handle_request""#.into(),
                on_empty: OnEmpty::Drop,
            })
            .with(Function::Contains {
                value: TypeValue::String("string".into()),
                set: vec![
                    TypeValue::String("string1".into()),
                    TypeValue::String("string2".into()),
                ],
            })
            .with(Function::Group {
                columns: vec!["host".into(), "_measurement".into()],
                mode: GroupMode::By,
            });

        let query2 = Query::new()
            .from("server".into())
            .range(1602404530510000000, 1602404530610000000)
            .filter(
                r#"(r) => r["_measurement"] == "handle_request""#.into(),
                OnEmpty::Drop,
            )
            .contains(
                TypeValue::String("string".into()),
                vec![
                    TypeValue::String("string1".into()),
                    TypeValue::String("string2".into()),
                ],
            )
            .group(vec!["host".into(), "_measurement".into()], GroupMode::By);

        assert_eq!(query1, query2);
    }
}
