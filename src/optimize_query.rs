use graphql_parser::{
    parse_query,
    query::{Definition, Document, OperationDefinition, Selection},
};

pub fn optimize_query<'a>(query: &'a str, used_col: u64) -> anyhow::Result<String> {
    let mut parse_result: Document<'a, &str> = parse_query(query)?;
    let Some(Definition::Operation(OperationDefinition::Query(query))) =
        parse_result.definitions.iter_mut().find(|it| match it {
            Definition::Operation(op) => match op {
                OperationDefinition::Query(_) => true,
                _ => false,
            },
            Definition::Fragment(_) => false,
        })
    else {
        return Err(anyhow::Error::msg("No query operation found"));
    };

    // get operation name
    let Some(item) = query.selection_set.items.first_mut() else {
        return Err(anyhow::Error::msg(format!(
            "At least on item in query expected",
        )));
    };
    let Selection::Field(field) = item else {
        return Err(anyhow::Error::msg(format!("Field expected ({:?})", item)));
    };

    let mut result_vars_idx = 0;
    // e.g. "... on AuthToken"
    for item in field.selection_set.items.iter_mut() {
        let Selection::InlineFragment(fragment) = item else {
            return Err(anyhow::Error::msg(format!(
                "InlineFragment expected for ({:?})",
                item
            )));
        };

        let mut items_to_remove = vec![];
        for (i, item) in fragment.selection_set.items.iter().enumerate() {
            let Selection::Field(_) = &item else {
                return Err(anyhow::Error::msg(format!(
                    "Only simple field supported ({:?})",
                    item
                )));
            };

            if (used_col >> result_vars_idx) & 1 == 0 {
                items_to_remove.push(i)
            }
            result_vars_idx = result_vars_idx + 1;
        }
        let items = &mut fragment.selection_set.items;
        for index_to_remove in items_to_remove.into_iter().rev() {
            // TODO handle case when we removed all items
            items.remove(index_to_remove);
        }
    }

    let output = format!("{parse_result}");
    println!("Request Query:");
    println!("{output}");
    Ok(output)
}
