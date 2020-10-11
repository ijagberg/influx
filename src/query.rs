use std::fmt::Display;

fn comma_join<T>(v: &[T]) -> String
where
    T: Display,
{
    v.iter()
        .map(|c| format!(r#""{}""#, c))
        .collect::<Vec<_>>()
        .join(", ")
}

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
    From {
        bucket: String,
    },
    Range {
        start: u128,
        stop: u128,
    },
    Filter {
        function: String,
        on_empty: OnEmpty,
    },
    Group {
        columns: Vec<String>,
        mode: GroupMode,
    },
    Yield {
        name: String,
    },
    Keep {
        columns: Vec<String>,
        function: String,
    },
    Drop {
        columns: Vec<String>,
        function: String,
    },
    Tail {
        n: u32,
        offset: u32,
    },
    Contains {
        value: TypeValue,
        set: Vec<TypeValue>,
    },
    Distinct {
        column: String,
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
                comma_join(columns),
                mode
            ),
            Function::Yield { name } => write!(f, r#"yield(name: "{}""#, name),
            Function::Keep { columns, function } => write!(
                f,
                r#"keep(columns: [{}], fn: {})"#,
                comma_join(columns),
                function
            ),
            Function::Drop { columns, function } => write!(
                f,
                r#"drop(columns: [{}], fn: {})"#,
                comma_join(columns),
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
        let query = Query::new()
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
            .with(Function::Group {
                columns: vec!["host".into(), "_measurement".into()],
                mode: GroupMode::By,
            });

        assert_eq!(&query.to_string(), "asd");
    }
}
