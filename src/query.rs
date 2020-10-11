use std::fmt::Display;

pub struct Query {
    lines: Vec<Method>,
}

impl Query {
    pub fn new(lines: Vec<Method>) -> Self {
        Self { lines }
    }

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.lines.is_empty() {
            write!(f, "")
        } else {
            write!(
                f,
                "{}",
                self.lines
                    .iter()
                    .map(|m| m.to_string())
                    .collect::<Vec<_>>()
                    .join("\n |> ")
            )
        }
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

pub enum Method {
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
}

impl Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Method::From { bucket } => write!(f, r#"from(bucket: "{}")"#, bucket),
            Method::Range { start, stop } => {
                write!(f, r#"range(start: {}, stop: {})"#, start, stop)
            }
            Method::Filter { function, on_empty } => {
                write!(f, r#"filter(fn: {}, onEmpty: "{}")"#, function, on_empty)
            }
            Method::Group { columns, mode } => write!(
                f,
                r#"group(columns: [{}], mode:"{}""#,
                columns
                    .iter()
                    .map(|c| format!(r#""{}""#, c))
                    .collect::<Vec<_>>()
                    .join(", "),
                mode
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
        let lines = vec![
            Method::From {
                bucket: "server".into(),
            },
            Method::Range {
                start: 1602404530510,
                stop: 1602404530610,
            },
            Method::Filter {
                function: r#"(r) => r["_measurement"] == "handle_request""#.into(),
                on_empty: OnEmpty::Drop,
            },
            Method::Group {
                columns: vec!["host".into(), "_measurement".into()],
                mode: GroupMode::By,
            },
        ];
        let query = Query::new(lines);

        assert_eq!(&query.to_string(), "asd");
    }
}
