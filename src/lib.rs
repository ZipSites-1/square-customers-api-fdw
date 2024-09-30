#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, time,
        types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
        utils,
    },
};

#[derive(Debug, Default)]
struct ExampleFdw {
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    access_token: String, // Add an access token field for Square API
}

// Pointer for the static FDW instance
static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();

impl ExampleFdw {
    // Initialize FDW instance
    fn init_instance() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }
}

impl Guest for ExampleFdw {
    fn host_version_requirement() -> String {
        // Semver expression for Wasm FDW host version requirement
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        // Get API URL and Access Token from foreign server options
        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("base_url", "https://connect.squareup.com/v2/customers");
        this.access_token = opts.require("access_token")?; // Fetch the access token for Square API

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Prepare URL for Square Customers API (no sheet_id, just customers)
        let opts = ctx.get_options(OptionsType::Table);
        let url = format!("{}", this.base_url); // Square API endpoint already includes '/customers'

        // Prepare the headers required for Square API (authorization)
        let headers: Vec<(String, String)> = vec![
            ("authorization".to_owned(), format!("Bearer {}", this.access_token)),
            ("content-type".to_owned(), "application/json".to_owned()),
            ("user-agent".to_owned(), "SquareCustomers FDW".to_owned()),
        ];

        // Make a request to Square API and parse response as JSON
        let req = http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        };

        let resp = http::get(&req)?;

        // Parse the JSON response body
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        // Extract customers from response
        this.src_rows = resp_json
            .get("customers")
            .ok_or("cannot find 'customers' field in the response")?
            .as_array()
            .ok_or("customers field is not an array")?
            .to_owned();

        // Output a Postgres INFO to user (visible in psql), also useful for debugging
        utils::report_info(&format!(
            "Retrieved {} customers from Square API.",
            this.src_rows.len()
        ));

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // If all source rows are consumed, stop data scan
        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }

        // Extract current customer row
        let src_row = &this.src_rows[this.src_idx];

        // Map Square API fields to target columns
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            let src_value = src_row.get(tgt_col_name); // Match JSON field names with column names

            let cell = match tgt_col.type_oid() {
                TypeOid::String => src_value.and_then(|v| v.as_str()).map(|v| Cell::String(v.to_owned())),
                TypeOid::I64 => src_value.and_then(|v| v.as_i64()).map(Cell::I64),
                TypeOid::Timestamp => src_value.and_then(|v| v.as_str()).and_then(|v| {
                    // Parse timestamp from string format
                    time::parse_from_rfc3339(v).ok().map(Cell::Timestamp)
                }),
                _ => return Err(format!("Column {} data type is not supported", tgt_col_name).into()),
            };

            // Push the cell to target row
            row.push(cell.as_ref());
        }

        // Advance to next source row
        this.src_idx += 1;

        // Tell Postgres we've done one row, and need to scan the next row
        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        Err("re_scan on foreign table is not supported".to_owned())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }
}

bindings::export!(ExampleFdw with_types_in bindings);



