use std::marker::PhantomData;
use std::os::raw::c_int;

use std::str;

use rusqlite::{
    ffi,
    types::Null,
    vtab::{
        parameter, read_only_module, Context, CreateVTab, IndexInfo, VTab, VTabConfig,
        VTabConnection, VTabCursor, VTabKind, Values,
    },
    Connection, Error, Result,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    optimize_query::optimize_query,
    parse_query::{parse, QueryDetails, ResultPath},
};

/// Register the "graphql" module.
/// ```sql
/// CREATE VIRTUAL TABLE vtab USING graphql(
/// graphql(url='http://localhost:8000/graphql',
///   url=SERVER_URL
///   operationName=OPERATION_NAME -- Query GraphGL operation name
///   query=GRAPHQL_QUERY -- The underlying graphql query
///   variableNames='[]' -- JSON array of input variable names
/// );
/// ```
pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("graphql", read_only_module::<GraphQLTab>(), aux)
}

#[derive(Default, Clone)]
struct Config {
    url: String,
    query: String,

    /// Values derived from the query string
    query_details: QueryDetails,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.url.is_empty() {
            return Err(Error::ModuleError("no server `url` specified".to_owned()));
        }
        if self.query.is_empty() {
            return Err(Error::ModuleError(
                "no Graphql `query` specified".to_owned(),
            ));
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ParameterDetail {
    /// column number of the parameter
    col: usize,
}

#[derive(Serialize, Deserialize)]
struct QueryInfo {
    pub params: Vec<ParameterDetail>,
    pub col_used: u64,
}

#[repr(C)]
struct GraphQLTab {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab,
    config: Config,
}

unsafe impl<'vtab> VTab<'vtab> for GraphQLTab {
    type Aux = ();
    type Cursor = GraphqlTabCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        args: &[&[u8]],
    ) -> Result<(String, GraphQLTab)> {
        if args.len() < 4 {
            return Err(Error::ModuleError("missing params".to_owned()));
        }

        let mut vtab = GraphQLTab {
            base: ffi::sqlite3_vtab::default(),
            config: Config::default(),
        };

        let args = &args[3..];
        for c_slice in args {
            let (param, value) = parameter(c_slice)?;
            match param {
                "url" => vtab.config.url = value.to_owned(),
                "query" => vtab.config.query = value.to_owned(),
                _ => {}
            }
        }

        vtab.config.validate()?;
        vtab.config.query_details =
            parse(&vtab.config.query).map_err(|err| Error::ModuleError(err.to_string()))?;

        let var_col_iter = vtab
            .config
            .query_details
            .variables
            .iter()
            .map(|it| it.name.clone());
        let result_col_iter = vtab
            .config
            .query_details
            .results
            .iter()
            .map(|it| it.to_string());
        let cols: Vec<String> = result_col_iter.chain(var_col_iter).collect();

        let mut sql = String::from("CREATE TABLE x(");
        for (i, col) in cols.iter().enumerate() {
            sql.push('"');
            sql.push_str(col);
            sql.push_str("\" TEXT");
            if i == cols.len() - 1 {
                sql.push_str(");");
            } else {
                sql.push_str(", ");
            }
        }

        db.config(VTabConfig::DirectOnly)?;
        Ok((sql, vtab))
    }

    // Only a forward full table scan is supported.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        let query_info = QueryInfo {
            params: info
                .constraints()
                .filter(|c| {
                    c.is_usable() && c.column() as usize >= self.config.query_details.results.len()
                })
                .map(|c| ParameterDetail {
                    col: c.column() as usize,
                })
                .collect::<Vec<_>>(),
            col_used: info.col_used(),
        };

        info.set_idx_str(&serde_json::to_string(&query_info).unwrap());

        // just request use all constraints
        for (i, _) in query_info.params.iter().enumerate() {
            info.constraint_usage(i).set_argv_index((i + 1) as c_int);
        }

        info.set_estimated_cost(1_000_000.);
        Ok(())
    }

    fn open(&mut self) -> Result<GraphqlTabCursor<'_>> {
        Ok(GraphqlTabCursor::new(self.config.clone()))
    }
}

impl CreateVTab<'_> for GraphQLTab {
    const KIND: VTabKind = VTabKind::Default;
}

#[repr(C)]
struct GraphqlTabCursor<'vtab> {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,

    config: Config,

    rows: Vec<Vec<serde_json::Value>>,
    row_number: usize,
    phantom: PhantomData<&'vtab GraphQLTab>,
}

impl GraphqlTabCursor<'_> {
    fn new<'vtab>(config: Config) -> GraphqlTabCursor<'vtab> {
        GraphqlTabCursor {
            base: ffi::sqlite3_vtab_cursor::default(),

            config,

            rows: vec![],
            row_number: 0,
            phantom: PhantomData,
        }
    }
}

unsafe impl VTabCursor for GraphqlTabCursor<'_> {
    // Only a full table scan is supported.  So `filter` simply rewinds to
    // the beginning.
    fn filter(&mut self, _idx_num: c_int, idx_str: Option<&str>, args: &Values<'_>) -> Result<()> {
        self.rows.clear();
        self.row_number = 0;

        let query_info = idx_str
            .map(|s| serde_json::from_str::<QueryInfo>(s).unwrap())
            .unwrap();
        let mut variables = serde_json::Map::new();
        for (i, param) in query_info.params.iter().enumerate() {
            let Some(config_param) = self
                .config
                .query_details
                .variables
                .get(param.col - self.config.query_details.results.len())
            else {
                continue;
            };
            variables.insert(
                config_param.name.clone(),
                serde_json::Value::String(args.get(i)?),
            );
        }

        let client = reqwest::blocking::Client::new();
        let query = optimize_query(&self.config.query, query_info.col_used)
            .map_err(|err| Error::ModuleError(err.to_string()))?;
        println!("API Request:\n{query}");
        let res = client
            .post(&self.config.url)
            .json(&json!({
              "operationName": self.config.query_details.operation_name,
              "query": query,
              "variables": variables
            }))
            .send()
            .map_err(|err| Error::ModuleError(err.to_string()))?
            .json::<serde_json::Value>()
            .map_err(|err| Error::ModuleError(err.to_string()))?;

        if let Some(errors) = res.get("errors") {
            return Err(Error::ModuleError(errors.to_string()));
        };

        let operation_result = res
            .get("data")
            .unwrap_or(&Value::Null)
            .get(&self.config.query_details.endpoint_name)
            .unwrap_or(&Value::Null);

        // find first array
        let mut found_array: Option<ResultPath> = None;
        let mut array_value: Option<&Value> = None;
        for expected_result in &self.config.query_details.results {
            let mut value = operation_result;
            let mut current = vec![];
            for path in &expected_result.path {
                current.push(path.clone());
                value = value.get(&path).unwrap_or(&Value::Null);
                if !value.is_array() {
                    continue;
                }
                found_array = Some(ResultPath { path: current });
                array_value = Some(value);
                break;
            }
            if found_array.is_some() {
                break;
            }
        }
        // build template row with none array elements
        let mut template_row = vec![];
        for expected_result in &self.config.query_details.results {
            if let Some(found_array) = &found_array {
                if expected_result.to_string() == found_array.to_string() {
                    template_row.push(Value::Null);
                    continue;
                }
            }
            let mut value = operation_result;
            for path in &expected_result.path {
                value = value.get(&path).unwrap_or(&Value::Null);
            }
            template_row.push(value.clone());
        }

        if found_array.is_none() {
            self.rows.push(template_row);
        } else {
            let array = array_value.unwrap().as_array().unwrap();
            let found_array = found_array.unwrap();
            let found_array_str = found_array.to_string();
            for (i, array_element) in array.iter().enumerate() {
                let mut row = vec![];
                for expected_result in &self.config.query_details.results {
                    if expected_result.to_string().starts_with(&found_array_str) {
                        let mut value = array_element;
                        for path in expected_result.path.iter().skip(found_array.path.len()) {
                            value = value.get(&path).unwrap_or(&Value::Null);
                        }
                        row.push(value.clone());
                    } else {
                        row.push(template_row[i].clone());
                    }
                }
                self.rows.push(row);
            }
        }

        // Fill in the query parameters
        for row in self.rows.iter_mut() {
            for (i, _) in self.config.query_details.variables.iter().enumerate() {
                let Some(parameter_idx) = query_info
                    .params
                    .iter()
                    .position(|d| d.col == i + self.config.query_details.results.len())
                else {
                    row.push(Value::Null);
                    continue;
                };
                row.push(serde_json::Value::String(args.get(parameter_idx)?));
            }
        }

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.row_number += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_number >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let columns = &self.rows[self.row_number];
        if col < 0 || col as usize >= columns.len() {
            return Err(Error::ModuleError(format!(
                "column index out of bounds: {col}"
            )));
        }
        if columns.is_empty() {
            return ctx.set_result(&Null);
        }
        ctx.set_result(&columns[col as usize].as_str().unwrap_or("").to_owned())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_number as i64)
    }
}

#[cfg(test)]
mod test {
    use crate::graphql;
    use fallible_iterator::FallibleIterator;
    use rusqlite::{Connection, Result};

    #[test]
    fn test_graphql_module() -> Result<()> {
        let db = Connection::open_in_memory()?;
        graphql::load_module(&db)?;
        let query_str = r#"
        query MyQuery($username_eq: String!, $password_eq: String!) {
          authToken(password: $password_eq, username: $username_eq) {
            ... on AuthToken {
              __typename
              token
            }
          }
        }
        "#;
        db.execute_batch(&format!(
            "CREATE VIRTUAL TABLE auth_token USING graphql(url='http://localhost:8000/graphql',
                query='{}')",
            query_str
        ))?;

        {
            let mut s =
                db.prepare("SELECT rowid, token, password_eq, username_eq FROM vtab WHERE password_eq = '' AND username_eq = ''")?;

            let results: Vec<String> = s.query([])?.map(|row| row.get::<_, String>(0)).collect()?;
            println!("Results: {results:?}");
            let token = results.get(0).unwrap().clone();
            assert!(!token.is_empty());
        }
        db.execute_batch("DROP TABLE auth_token")?;
        Ok(())
    }

    #[test]
    fn test_list_query() -> Result<()> {
        let db = Connection::open_in_memory()?;
        graphql::load_module(&db)?;
        let query_str = r#"
        query Query {
            allFilms {
                films {
                    id
                    title
                    director
                    edited
                }
            }
        }
        "#;
        db.execute_batch(&format!(
            "CREATE VIRTUAL TABLE films USING graphql(url='https://swapi-graphql.netlify.app/.netlify/functions/index',
                query='{}')",
            query_str
        ))?;

        {
            let mut s = db.prepare("SELECT * FROM films")?;

            let results: Vec<String> = s.query([])?.map(|row| row.get::<_, String>(1)).collect()?;
            println!("Results: {results:?}");
            assert_eq!(results.len(), 6);
        }
        db.execute_batch("DROP TABLE films")?;
        Ok(())
    }
}
