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

    /// Create a query from a raw string.
    ///
    /// ## Example
    /// ```rust
    /// let query = Query::raw(r#"from(bucket: "server")
    ///     |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    ///     |> filter(fn: (r) => r["_measurement"] == "m1")
    ///     |> keys()"#);
    /// ```
    pub fn raw(query: String) -> Self {
        let lines = query.lines().map(|l| l.to_owned()).collect();
        Self { lines }
    }

    #[allow(unused)]
    fn with(mut self, function: Function) -> Self {
        self.lines.push(function.to_string());
        self
    }

    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.lines.push(text.into());
        self
    }

    /// Used to retrieve data from an InfluxDB data source.
    /// It returns a stream of tables from the specified bucket.
    /// Each unique series is contained within its own table.
    /// Each record in the table represents a single point in the series.
    ///
    /// ## Params
    /// * `bucket`: Name of the bucket to query.
    pub fn from(mut self, bucket: impl Into<String>) -> Self {
        self.lines.push(
            Function::From {
                bucket: bucket.into(),
            }
            .to_string(),
        );
        self
    }

    /// Filters records based on time bounds.
    /// Each input table's records are filtered to contain only records that exist within the time bounds.
    /// Each input table's group key value is modified to fit within the time bounds.
    /// Tables where all records exists outside the time bounds are filtered entirely.
    ///
    /// ## Params
    /// * `start`: The earliest time to include in results.
    /// * `stop`: The latest time to include in results. Defaults to `now()`.
    pub fn range(mut self, start: u128, stop: Option<u128>) -> Self {
        self.lines.push(Function::Range { start, stop }.to_string());
        self
    }

    /// Filters data based on conditions defined in the function.
    /// The output tables have the same schema as the corresponding input tables.
    ///
    /// ## Params
    /// * `function`: A single argument function that evaluates true or false. Records are passed to the function. Those that evaluate to true are included in the output tables.
    /// * `on_empty`: Defines the behavior for empty tables. Potential values are `keep` and `drop`. Defaults to `drop`.
    pub fn filter(mut self, function: impl Into<String>, on_empty: Option<OnEmpty>) -> Self {
        self.lines.push(
            Function::Filter {
                function: function.into(),
                on_empty,
            }
            .to_string(),
        );
        self
    }

    /// Groups records based on their values for specific columns.
    /// It produces tables with new group keys based on provided properties.
    ///
    /// ## Params
    /// * `columns`: List of columns to use in the grouping operation. Defaults to `[]`.
    /// * `mode`: The mode used to group columns. The following options are available: by, except. Defaults to `"by"`.
    pub fn group(mut self, columns: Vec<String>, mode: Option<GroupMode>) -> Self {
        self.lines
            .push(Function::Group { columns, mode }.to_string());
        self
    }

    /// Indicates the input tables received should be delivered as a result of the query.
    /// Yield outputs the input stream unmodified.
    /// A query may have multiple results, each identified by the name provided to the `yield()` function.
    ///
    /// ## Params
    /// * `name`: A unique name for the yielded results.
    pub fn r#yield(mut self, name: impl Into<String>) -> Self {
        self.lines
            .push(Function::Yield { name: name.into() }.to_string());
        self
    }

    /// Returns a table containing only the specified columns, ignoring all others.
    /// Only columns in the group key that are also specified in the `keep()` function will be kept in the resulting group key.
    /// It is the inverse of `drop`.
    ///
    /// ## Params
    /// * `columns`: Columns that should be included in the resulting table. Cannot be used with `function`.
    /// * `function`: A predicate function which takes a column name as a parameter and returns a boolean indicating whether or not the column should be removed from the table. Cannot be used with `columns`.
    pub fn keep(
        mut self,
        columns: Option<Vec<String>>,
        function: Option<impl Into<String>>,
    ) -> Self {
        self.lines.push(
            Function::Keep {
                columns,
                function: function.map(|f| f.into()),
            }
            .to_string(),
        );
        self
    }

    /// Removes specified columns from a table.
    /// Columns can be specified either through a list or a predicate function.
    /// When a dropped column is part of the group key, it will be removed from the key.
    ///
    /// ## Params
    /// * `columns`: A list of columns to be removed from the table. Cannot be used with `function`.
    /// * `function`: A function which takes a column name as a parameter and returns a boolean indicating whether or not the column should be removed from the table. Cannot be used with `columns`.
    pub fn drop(
        mut self,
        columns: Option<Vec<String>>,
        function: Option<impl Into<String>>,
    ) -> Self {
        self.lines.push(
            Function::Drop {
                columns,
                function: function.map(|f| f.into()),
            }
            .to_string(),
        );
        self
    }

    /// Limits each output table to the last `n` records, excluding the offset.
    ///
    /// ## Params
    /// * `n`: The maximum number of records to output.
    /// * `offset`: The number of records to skip at the end of a table before limiting to `n`. Defaults to `0`.
    pub fn tail(mut self, n: u32, offset: Option<u32>) -> Self {
        self.lines.push(Function::Tail { n, offset }.to_string());
        self
    }

    /// Tests whether a value is a member of a set.
    ///
    /// ## Params
    /// * `value`: The value to search for.
    /// * `set`: The set of values in which to search.
    ///
    /// ## Example
    /// `contains(value: 1, set: [1,2,3])`
    pub fn contains<T>(mut self, value: T, set: Vec<T>) -> Self
    where
        TypeValue: From<T>,
    {
        self.lines.push(
            Function::Contains {
                value: value.into(),
                set: set.into_iter().map(TypeValue::from).collect(),
            }
            .to_string(),
        );
        self
    }

    /// Returns the unique values for a given column.
    ///
    /// ## Params
    /// * `column`: Column on which to track unique values.
    ///
    /// ## Example
    /// `distinct(column: "host")`
    pub fn distinct(mut self, column: impl Into<String>) -> Self {
        self.lines.push(
            Function::Distinct {
                column: column.into(),
            }
            .to_string(),
        );
        self
    }

    /// Selects record with the highest `_value` from the input table.
    pub fn max(mut self) -> Self {
        self.lines.push(Function::Max.to_string());
        self
    }

    /// Selects record with the lowest `_value` from the input table.
    pub fn min(mut self) -> Self {
        self.lines.push(Function::Min.to_string());
        self
    }

    /// Limits each output table to the first `n` records, excluding the offset.
    ///
    /// ## Params
    /// * `n`: The maximum number of records to output.
    /// * `offset`: The number of records to skip at the beginning of a table before limiting to `n`. Defaults to `0`.
    pub fn limit(mut self, n: u32, offset: Option<u32>) -> Self {
        self.lines.push(Function::Limit { n, offset }.to_string());
        self
    }

    /// Assigns a static value to each record in the input table.
    /// The key may modify an existing column or add a new column to the tables.
    /// If the modified column is part of the group key, the output tables are regrouped as needed.
    ///
    /// ## Params
    /// * `key`: The label of the column to modify or set.
    /// * `value`: The string value to set.
    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.lines.push(
            Function::Set {
                key: key.into(),
                value: value.into(),
            }
            .to_string(),
        );
        self
    }

    /// Orders the records within each table.
    /// One output table is produced for each input table.
    /// The output tables will have the same schema as their corresponding input tables.
    ///
    /// ## Params
    /// * `columns`: List of columns by which to sort. Sort precedence is determined by list order (left to right). Default is `["_value"]`.
    /// * `desc`: Sort results in descending order. Default is `false`.
    pub fn sort(mut self, columns: Option<Vec<String>>, desc: Option<bool>) -> Self {
        self.lines
            .push(Function::Sort { columns, desc }.to_string());
        self
    }

    /// Outputs the number of records in the specified column.
    ///
    /// ## Params
    /// * `column`: The column on which to operate. Defaults to `"_value"`.
    pub fn count(mut self, column: Option<impl Into<String>>) -> Self {
        self.lines.push(
            Function::Count {
                column: column.map(|c| c.into()),
            }
            .to_string(),
        );
        self
    }

    /// Returns a list of buckets in the organization.
    pub fn buckets(mut self) -> Self {
        self.lines.push(Function::Buckets.to_string());
        self
    }

    /// Computes the area under the curve per unit of time of subsequent non-null records.
    /// The curve is defined using `_time` as the domain and record values as the range.
    ///
    /// ## Params
    /// * `unit`: The time duration used when computing the integral.
    /// * `column`: The column on which to operate. Defaults to `"_value"`.
    /// * `time_column`: Column that contains time values to use in the operation. Defaults to `"_time"`.
    pub fn integral(
        mut self,
        unit: impl Into<String>,
        column: Option<impl Into<String>>,
        time_column: Option<impl Into<String>>,
    ) -> Self {
        self.lines.push(
            Function::Integral {
                unit: unit.into(),
                column: column.map(|c| c.into()),
                time_column: time_column.map(|t| t.into()),
            }
            .to_string(),
        );
        self
    }

    /// Duplicates a specified column in a table.
    ///
    /// ## Params
    /// * `column`: The column name to duplicate.
    /// * `as`: The name assigned to the duplicate column.
    pub fn duplicate(mut self, column: impl Into<String>, r#as: impl Into<String>) -> Self {
        self.lines.push(
            Function::Duplicate {
                column: column.into(),
                r#as: r#as.into(),
            }
            .to_string(),
        );
        self
    }

    /// Outputs the group key of input tables.
    /// For each input table, it outputs a table with the same group key columns,
    /// plus a _value column containing the labels of the input table's group key.
    ///
    /// ## Params
    /// * `column`: Column is the name of the output column to store the group key labels. Defaults to `_value`.
    pub fn keys(mut self, column: Option<impl Into<String>>) -> Self {
        self.lines.push(
            Function::Keys {
                column: column.map(|c| c.into()),
            }
            .to_string(),
        );

        self
    }

    /// Collects values stored vertically (column-wise) in a table and aligns them horizontally (row-wise) into logical sets.
    ///
    /// ## Params
    /// * `row_key`: List of columns used to uniquely identify a row for the output.
    /// * `column_key`: List of columns used to pivot values onto each row identified by the rowKey.
    /// * `value_column`: The single column that contains the value to be moved around the pivot.
    pub fn pivot(
        mut self,
        row_key: Vec<String>,
        column_key: Vec<String>,
        value_column: String,
    ) -> Self {
        self.lines.push(
            Function::Pivot {
                row_key,
                column_key,
                value_column,
            }
            .to_string(),
        );

        self
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
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

enum Function {
    From {
        bucket: String,
    },
    Range {
        start: u128,
        stop: Option<u128>,
    },
    Filter {
        function: String,
        on_empty: Option<OnEmpty>,
    },
    Group {
        columns: Vec<String>,
        mode: Option<GroupMode>,
    },
    Yield {
        name: String,
    },
    Keep {
        columns: Option<Vec<String>>,
        function: Option<String>,
    },
    Drop {
        columns: Option<Vec<String>>,
        function: Option<String>,
    },
    Tail {
        n: u32,
        offset: Option<u32>,
    },
    Contains {
        value: TypeValue,
        set: Vec<TypeValue>,
    },
    Distinct {
        column: String,
    },
    Max,
    Min,
    Limit {
        n: u32,
        offset: Option<u32>,
    },
    Set {
        key: String,
        value: String,
    },
    Sort {
        columns: Option<Vec<String>>,
        desc: Option<bool>,
    },
    Count {
        column: Option<String>,
    },
    Buckets,
    Integral {
        unit: String,
        column: Option<String>,
        time_column: Option<String>,
    },
    Duplicate {
        column: String,
        r#as: String,
    },
    Keys {
        column: Option<String>,
    },
    Pivot {
        row_key: Vec<String>,
        column_key: Vec<String>,
        value_column: String,
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
            Function::Range { start, stop } => match stop {
                Some(stop) => {
                    write!(f, r#"range(start: {}, stop: {})"#, start, stop)
                }
                None => {
                    write!(f, r#"range(start: {})"#, start)
                }
            },
            Function::Filter { function, on_empty } => match on_empty {
                Some(on_empty) => {
                    write!(f, r#"filter(fn: {}, onEmpty: "{}")"#, function, on_empty)
                }
                None => {
                    write!(f, r#"filter(fn: {})"#, function)
                }
            },
            Function::Group { columns, mode } => match mode {
                Some(mode) => {
                    write!(
                        f,
                        r#"group(columns: [{}], mode:"{}")"#,
                        comma_join_strings(columns),
                        mode
                    )
                }
                None => {
                    write!(f, r#"group(columns: [{}])"#, comma_join_strings(columns),)
                }
            },
            Function::Yield { name } => write!(f, r#"yield(name: "{}")"#, name),
            Function::Keep { columns, function } => match (columns, function) {
                (None, Some(function)) => {
                    write!(f, r#"keep(fn: "{}")"#, function)
                }
                (Some(columns), None) => {
                    write!(f, r#"keep(columns: [{}])"#, comma_join_strings(columns),)
                }
                _ => panic!("invalid instance of `Function::Keep`"),
            },

            Function::Drop { columns, function } => match (columns, function) {
                (Some(columns), None) => {
                    write!(f, r#"drop(columns: [{}])"#, comma_join_strings(columns),)
                }
                (None, Some(function)) => {
                    write!(f, r#"drop(fn: "{}")"#, function)
                }
                _ => panic!("invalid instance of `Function::Keep`"),
            },
            Function::Tail { n, offset } => match offset {
                Some(offset) => {
                    write!(f, r#"tail(n: {}, offset: {})"#, n, offset)
                }
                None => {
                    write!(f, r#"tail(n: {})"#, n)
                }
            },
            Function::Contains { value, set } => write!(
                f,
                r#"contains(value: {}, set: [{}])"#,
                value,
                comma_join(set)
            ),
            Function::Distinct { column } => write!(f, r#"distinct(column: "{}")"#, column),
            Function::Max => write!(f, "max()"),
            Function::Min => write!(f, "min()"),
            Function::Limit { n, offset } => match offset {
                Some(offset) => {
                    write!(f, r#"limit(n: {}, offset: {})"#, n, offset)
                }
                None => {
                    write!(f, r#"limit(n: {})"#, n)
                }
            },
            Function::Set { key, value } => write!(f, r#"set(key: "{}", value: "{}")"#, key, value),
            Function::Sort { columns, desc } => match (columns, desc) {
                (None, None) => {
                    write!(f, r#"sort()"#)
                }
                (None, Some(desc)) => {
                    write!(f, r#"sort(desc: {}"#, desc)
                }
                (Some(columns), None) => {
                    write!(f, r#"sort(columns: [{}])"#, comma_join_strings(columns))
                }
                (Some(columns), Some(desc)) => {
                    write!(
                        f,
                        r#"sort(columns: [{}], desc: {})"#,
                        comma_join_strings(columns),
                        desc
                    )
                }
            },
            Function::Count { column } => match column {
                Some(column) => {
                    write!(f, r#"count(column: "{}")"#, column)
                }
                None => {
                    write!(f, r#"count()"#)
                }
            },
            Function::Buckets => write!(f, r#"buckets()"#),
            Function::Integral {
                unit,
                column,
                time_column,
            } => match (column, time_column) {
                (None, None) => {
                    write!(f, r#"integral(unit: {})"#, unit)
                }
                (None, Some(time_column)) => {
                    write!(
                        f,
                        r#"integral(unit: {}, timeColumn: "{}")"#,
                        unit, time_column
                    )
                }
                (Some(column), None) => {
                    write!(f, r#"integral(unit: {}, column: "{}")"#, unit, column)
                }
                (Some(column), Some(time_column)) => {
                    write!(
                        f,
                        r#"integral(unit: {}, column: "{}", timeColumn: "{}")"#,
                        unit, column, time_column
                    )
                }
            },

            Function::Duplicate { column, r#as } => {
                write!(f, r#"duplicate(column: "{}", as: "{}")"#, column, r#as)
            }
            Function::Keys { column } => match column {
                Some(column) => {
                    write!(f, r#"keys(column: "{}")"#, column)
                }
                None => {
                    write!(f, r#"keys()"#)
                }
            },
            Function::Pivot {
                row_key,
                column_key,
                value_column,
            } => {
                write!(
                    f,
                    r#"pivot(rowKey: [{}], columnKey: [{}], valueColumn: "{}")"#,
                    comma_join_strings(row_key),
                    comma_join_strings(column_key),
                    value_column
                )
            }
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
                stop: Some(1602404530610000000),
            })
            .with(Function::Filter {
                function: r#"(r) => r["_measurement"] == "handle_request""#.into(),
                on_empty: Some(OnEmpty::Drop),
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
                mode: Some(GroupMode::By),
            });

        let query2 = Query::new()
            .from("server")
            .range(1602404530510000000, Some(1602404530610000000))
            .filter(
                r#"(r) => r["_measurement"] == "handle_request""#,
                Some(OnEmpty::Drop),
            )
            .contains(
                TypeValue::String("string".into()),
                vec![
                    TypeValue::String("string1".into()),
                    TypeValue::String("string2".into()),
                ],
            )
            .group(
                vec!["host".into(), "_measurement".into()],
                Some(GroupMode::By),
            );

        assert_eq!(query1, query2);
    }
}
