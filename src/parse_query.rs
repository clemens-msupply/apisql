use async_graphql_parser::{
    parse_query,
    types::{
        DocumentOperations, ExecutableDocument, OperationDefinition, OperationType, Selection,
    },
};

#[derive(Default, Clone)]
pub struct Variable {
    pub name: String,
    pub nullable: bool,
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
    let result = parse_query(full_query)?;
    let (op, operation_name, variables) = parse_query_variable(&result)?;
    let (endpoint_name, results) = parse_query_results(&op)?;
    Ok(QueryDetails {
        operation_name,
        endpoint_name,
        variables,
        results,
    })
}

fn parse_query_variable(
    full_query: &ExecutableDocument,
) -> anyhow::Result<(OperationDefinition, String, Vec<Variable>)> {
    let DocumentOperations::Multiple(operations) = &full_query.operations else {
        return Err(anyhow::Error::msg(
            "Only named queries supported for now...",
        ));
    };
    if operations.len() != 1 {
        return Err(anyhow::Error::msg(
            "Query must contain exactly one custom query",
        ));
    }
    let (operation_name, op) = operations.iter().next().unwrap();
    if op.node.ty != OperationType::Query {
        return Err(anyhow::Error::msg(format!("Operation must be a query")));
    };
    let vars = op
        .node
        .variable_definitions
        .iter()
        .map(|it| {
            let var = &it.node;
            Variable {
                name: var.name.node.to_string(),
                nullable: var.var_type.node.nullable,
            }
        })
        .collect();
    Ok((op.node.clone(), operation_name.to_string(), vars))
}

fn parse_query_results(op: &OperationDefinition) -> anyhow::Result<(String, Vec<ResultPath>)> {
    let mut out = Vec::new();

    if op.selection_set.node.items.len() != 1 {
        return Err(anyhow::Error::msg(format!(
            "Exactly one operation expected"
        )));
    }

    // get operation name
    let item = &op.selection_set.node.items.first().unwrap().node;
    let Selection::Field(field) = item else {
        return Err(anyhow::Error::msg(format!("Field expected ({:?})", item)));
    };
    let operation_name = field.node.name.node.to_string();

    // e.g. "... on AuthToken"
    for item in &field.node.selection_set.node.items {
        let Selection::InlineFragment(fragment) = &item.node else {
            return Err(anyhow::Error::msg(format!(
                "InlineFragment expected for ({:?})",
                item
            )));
        };

        for item in &fragment.node.selection_set.node.items {
            let Selection::Field(field) = &item.node else {
                return Err(anyhow::Error::msg(format!(
                    "Only simple field supported ({:?})",
                    item
                )));
            };

            // TODO support nested path
            out.push(ResultPath {
                path: vec![field.node.name.node.to_string()],
            });
        }
    }

    Ok((operation_name, out))
}
