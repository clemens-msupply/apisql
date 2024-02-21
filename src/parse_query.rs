use graphql_parser::query::{
    parse_query, Definition, Document, OperationDefinition, Query, Selection, SelectionSet,
};

#[derive(Default, Clone)]
pub struct Variable {
    pub name: String,
}

/// Path relative to the query
#[derive(Default, Clone)]
pub struct ResultPath {
    pub path: Vec<String>,
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

fn collect_fields<'a>(
    selection_set: &SelectionSet<'a, &'a str>,
    base: &Vec<String>,
) -> anyhow::Result<Vec<ResultPath>> {
    let mut out = Vec::new();
    for item in &selection_set.items {
        match item {
            Selection::Field(field) => {
                let mut path = base.clone();
                path.push(field.name.to_string());
                if field.selection_set.items.is_empty() {
                    out.push(ResultPath { path });
                } else {
                    out.append(&mut collect_fields(&field.selection_set, &path)?);
                }
            }
            Selection::InlineFragment(inline_fragment) => {
                // e.g. "... on AuthToken"
                let mut paths = collect_fields(&inline_fragment.selection_set, base)?;
                out.append(&mut paths);
            }
            Selection::FragmentSpread(_) => {
                return Err(anyhow::Error::msg(format!("FragmentSpread not supported")));
            }
        };
    }
    Ok(out)
}

fn parse_query_results<'a>(op: &Query<'a, &'a str>) -> anyhow::Result<(String, Vec<ResultPath>)> {
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

    let out = collect_fields(&field.selection_set, &vec![])?;
    Ok((operation_name, out))
}
