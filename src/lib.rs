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
use env_logger;
use log::{info, error, debug};
use std::sync::Once;

#[derive(Debug, Default)]
struct ExampleFdw {
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    access_token: String, // Store access token for reuse
}

// Pointer for the static FDW instance
static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();
static INIT: Once = Once::new();

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
        // Ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        // Ensure initialization happens only once
        INIT.call_once(|| {
            Self::init_instance();
        });
        let this = Self::this_mut();

        // Initialize logger safely
        if let Err(e) = env_logger::try_init() {
            // Logger was already initialized
            debug!("Logger initialization skipped: {}", e);
        } else {
            info!("Logger initialized successfully.");
        }

        info!("Initializing FDW...");

        // Fetch options from the server context
        let opts = ctx.get_options(OptionsType::Server);

        // Fetch the API URL and access token from options
        this.base_url = opts.require_or("api_url", "https://connect.squareup.com/v2/customers");
        this.access_token = opts.require_or("access_token", "your_default_token");

        // Log the base URL without exposing the access token
        utils::report_info(&format!("Using API base URL: {}", this.base_url));
        utils::report_info(&format!(
            "Access token received: {}****",
            &this.access_token[..5.min(this.access_token.len())] // Prevents panic if token is shorter
        )); // Masking for security

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
    
        let opts = ctx.get_options(OptionsType::Table);
        let object = opts.require("object")?;
        let mut url = format!("{}/{}", this.base_url, object);
    
        let headers: Vec<(String, String)> = vec![
            ("authorization".to_owned(), format!("Bearer {}", this.access_token)),
            ("content-type".to_owned(), "application/json".to_owned()),
            ("user-agent".to_owned(), "SquareCustomers FDW".to_owned()),
        ];
    
        let mut all_customers = Vec::new(); // Vector to store all customers across pages
        let mut cursor: Option<String> = None;
    
        loop {
            let req = http::Request {
                method: http::Method::Get,
                url: if let Some(ref c) = cursor {
                    format!("{}?cursor={}", url, c) // Append cursor to URL if it exists
                } else {
                    url.clone() // First request, no cursor
                },
                headers: headers.clone(),
                body: String::default(),
            };
    
            // Make the API request
            let resp = http::get(&req).map_err(|e| {
                error!("HTTP request failed: {}", e);
                e.to_string()
            })?;
    
            // Check if the status code is 200 (OK)
            if resp.status_code != 200 {
                error!("Non-200 response received: {}", resp.status_code);
                return Err(format!("Non-200 response received: {}", resp.status_code).into());
            }
    
            // Parse the JSON response body
            let resp_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| format!("JSON parsing error: {}", e))?;
    
            // Extract the 'customers' field from the response, expect it to be an array
            let customers = match resp_json.get("customers").and_then(|v| v.as_array()) {
                Some(array) => array.clone(),
                None => {
                    error!(
                        "Expected 'customers' field with an array in the response, but got: {:?}",
                        resp_json
                    );
                    return Err("Expected 'customers' field with an array in the response".into());
                }
            };
    
            // Add the current page of customers to the full list
            all_customers.extend(customers);
    
            // Log the number of customers retrieved so far
            utils::report_info(&format!(
                "Retrieved {} customers so far",
                all_customers.len()
            ));
    
            // Check if a pagination cursor exists in the response
            cursor = resp_json.get("cursor").and_then(|v| v.as_str().map(|s| s.to_owned()));
    
            if cursor.is_none() {
                // If no cursor is found, it means there are no more pages, so we break the loop
                break;
            } else {
                utils::report_info(&format!(
                    "More customers available, continuing with cursor: {}",
                    cursor.as_ref().unwrap()
                ));
            }
        }
    
        // Assign all the customers retrieved to the source rows for iteration
        this.src_rows = all_customers;
    
        // Log the total number of customers fetched
        utils::report_info(&format!(
            "Total customers retrieved from API: {}",
            this.src_rows.len()
        ));
    
        Ok(())
    }
    

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }

        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            let src = src_row
                .as_object()
                .and_then(|v| v.get(&tgt_col_name))
                .ok_or(format!("source column '{}' not found", tgt_col_name))?;
            let cell = match tgt_col.type_oid() {
                TypeOid::Bool => src.as_bool().map(Cell::Bool),
                TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
                TypeOid::Timestamp => {
                    if let Some(s) = src.as_str() {
                        let ts = time::parse_from_rfc3339(s)?;
                        Some(Cell::Timestamp(ts))
                    } else {
                        None
                    }
                }
                TypeOid::Json => src.as_object().map(|_| Cell::Json(src.to_string())),
                _ => {
                    return Err(format!(
                        "column '{}' data type is not supported",
                        tgt_col_name
                    )
                    .into());
                }
            };

            if let Some(c) = cell {
                row.push(Some(&c)); // Wrapped in Some as per expected type
            } else {
                return Err(format!(
                    "Unsupported data type for column '{}'",
                    tgt_col_name
                )
                .into());
            }
        }

        this.src_idx += 1;

        Ok(Some(0)) // Assuming 0 is an appropriate return value
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        Err("re_scan on foreign table is not supported".to_owned())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0; // Reset index for potential future scans
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err("insert operation is not supported".to_owned())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err("update operation is not supported".to_owned())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("delete operation is not supported".to_owned())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }
}

bindings::export!(ExampleFdw with_types_in bindings);


