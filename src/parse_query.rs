use async_graphql_parser::{
    parse_query,
    types::{DocumentOperations, OperationType},
};

#[derive(Default, Clone)]
pub struct Variable {
    pub name: String,
    pub nullable: bool,
}

pub fn parse_query_variable(full_query: &str, operation: &str) -> anyhow::Result<Vec<Variable>> {
    let result = parse_query(full_query)?;

    let DocumentOperations::Multiple(mut operations) = result.operations else {
        return Err(anyhow::Error::msg(
            "Only named queries supported for now...",
        ));
    };
    let Some(op) = operations.remove(operation) else {
        return Err(anyhow::Error::msg(format!(
            "Operation \"{operation}\" not found"
        )));
    };
    if op.node.ty != OperationType::Query {
        return Err(anyhow::Error::msg(format!("Operation must be a query")));
    };
    let vars = op
        .node
        .variable_definitions
        .into_iter()
        .map(|it| {
            let var = it.node;
            Variable {
                name: var.name.node.to_string(),
                nullable: var.var_type.node.nullable,
            }
        })
        .collect();
    Ok(vars)
}
