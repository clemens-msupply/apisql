use graphql_parser::query::{
    parse_query, Definition, Document, OperationDefinition, Query, Selection,
};

#[derive(Default, Clone)]
pub struct Variable {
    pub name: String,
}

/// Path relative to the query
#[derive(Default, Clone)]
pub struct ResultPath {
    path: Vec<String>,
}

impl ResultPath {
    pub fn to_string(&self) -> String {
        self.path.join("_")
    }
}

#[derive(Default, Clone)]
pub struct QueryDetails {
    pub operation_name: String,
    pub endpoint_name: String,
    pub variables: Vec<Variable>,
    pub results: Vec<ResultPath>,
}

pub fn parse(full_query: &str) -> anyhow::Result<QueryDetails> {
    let mut result = parse_query(full_query)?;
    let (op, operation_name, variables) = parse_query_variable(&mut result)?;
    let (endpoint_name, results) = parse_query_results(&op)?;
    Ok(QueryDetails {
        operation_name,
        endpoint_name,
        variables,
        results,
    })
}

pub fn parse_query_variable<'a>(
    full_query: &'a Document<'a, &'a str>,
) -> anyhow::Result<(&'a Query<'a, &'a str>, String, Vec<Variable>)> {
    let Some(Definition::Operation(OperationDefinition::Query(query_op))) =
        full_query.definitions.iter().find(|it| match it {
            Definition::Operation(op) => match op {
                OperationDefinition::Query(_) => true,
                _ => false,
            },
            Definition::Fragment(_) => false,
        })
    else {
        return Err(anyhow::Error::msg("No query operation found"));
    };

    let Some(operation_name) = query_op.name.map(|it| it.to_string()) else {
        return Err(anyhow::Error::msg("Missing operation name"));
    };

    let vars = query_op
        .variable_definitions
        .iter()
        .map(|var| Variable {
            name: var.name.to_string(),
        })
        .collect();
    Ok((query_op, operation_name.to_string(), vars))
}

fn parse_query_results<'a>(op: &Query<'a, &'a str>) -> anyhow::Result<(String, Vec<ResultPath>)> {
    let mut out = Vec::new();

    if op.selection_set.items.len() != 1 {
        return Err(anyhow::Error::msg(format!(
            "Exactly one operation expected"
        )));
    }

    // get operation name
    let item = op.selection_set.items.first().unwrap();
    let Selection::Field(field) = item else {
        return Err(anyhow::Error::msg(format!("Field expected ({:?})", item)));
    };
    let operation_name = field.name.to_string();

    // e.g. "... on AuthToken"
    for item in &field.selection_set.items {
        let Selection::InlineFragment(fragment) = &item else {
            return Err(anyhow::Error::msg(format!(
                "InlineFragment expected for ({:?})",
                item
            )));
        };

        for item in &fragment.selection_set.items {
            let Selection::Field(field) = &item else {
                return Err(anyhow::Error::msg(format!(
                    "Only simple field supported ({:?})",
                    item
                )));
            };

            // TODO support nested path
            out.push(ResultPath {
                path: vec![field.name.to_string()],
            });
        }
    }

    Ok((operation_name, out))
}
