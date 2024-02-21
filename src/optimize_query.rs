use graphql_parser::{
    parse_query,
    query::{Definition, Document, OperationDefinition, Selection, SelectionSet},
};

fn optimize_fields<'a>(
    selection_set: &mut SelectionSet<'a, &'a str>,
    base: &Vec<String>,
    used_col: u64,
    result_vars_idx: &mut i32,
) -> anyhow::Result<()> {
    let mut items_to_remove = Vec::new();
    for (i, item) in selection_set.items.iter_mut().enumerate() {
        match item {
            Selection::Field(field) => {
                let mut path = base.clone();
                path.push(field.name.to_string());
                if field.selection_set.items.is_empty() {
                    if (used_col >> *result_vars_idx) & 1 == 0 {
                        items_to_remove.push(i)
                    }
                    *result_vars_idx = *result_vars_idx + 1;
                } else {
                    optimize_fields(&mut field.selection_set, &path, used_col, result_vars_idx)?;
                }
            }
            Selection::InlineFragment(inline_fragment) => {
                // e.g. "... on AuthToken"
                optimize_fields(
                    &mut inline_fragment.selection_set,
                    base,
                    used_col,
                    result_vars_idx,
                )?;
            }
            Selection::FragmentSpread(_) => {
                return Err(anyhow::Error::msg(format!("FragmentSpread not supported")));
            }
        };
    }

    let items = &mut selection_set.items;
    for index_to_remove in items_to_remove.into_iter().rev() {
        // TODO handle this case better?
        // Don't remove all items to keep the query valid
        if items.len() == 1 {
            break;
        }
        items.remove(index_to_remove);
    }
    Ok(())
}

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
    optimize_fields(
        &mut field.selection_set,
        &vec![],
        used_col,
        &mut result_vars_idx,
    )?;

    let output = format!("{parse_result}");
    Ok(output)
}
