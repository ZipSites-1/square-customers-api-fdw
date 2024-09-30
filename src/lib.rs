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

#[derive(Debug, Default)]
struct ExampleFdw {
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

// pointer for the static FDW instance
static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();

impl ExampleFdw {
    // initialise FDW instance
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
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();
        env_logger::init(); // Initialize logger
        info!("Initializing FDW...");

        // Fetch options from the server context
        let opts = ctx.get_options(OptionsType::Server);

        // Fetch the Square API URL and access token from options
        this.base_url = opts.require_or("api_url", "https://connect.squareup.com/v2/customers");
        let access_token = opts.require_or("access_token", "your_default_token");

        // Save the access token into the FDW instance for later use
        utils::report_info(&format!("Using Square API base URL: {}", this.base_url));
        utils::report_info(&format!(
            "Access token received: {}****",
            &this.access_token[..5]
        ));
        
        Ok(())
    }

    use log::{info, debug};  // Removed 'error' since it is not used

fn begin_scan(ctx: &Context) -> FdwResult {
    let this = Self::this_mut();

    // Retrieve the options from the Table context
    let opts = ctx.get_options(OptionsType::Table);
    let object = opts.require("object")?;

    // Construct the URL for the API request (Square's Customers endpoint)
    let url = format!("{}/{}", this.base_url, object);

    // Fetch the access_token from the server options (retrieved in init)
    let opts_server = ctx.get_options(OptionsType::Server);
    let access_token = opts_server.require_or("access_token", "your_default_token");

     // Corrected header names to lowercase and included authorization
     let headers: Vec<(String, String)> = vec![
        ("authorization".to_owned(), format!("Bearer {}", this.access_token)),
        ("content-type".to_owned(), "application/json".to_owned()),
        ("user-agent".to_owned(), "SquareCustomers FDW".to_owned()),
    ];

    // Create the HTTP GET request to the Square API
    let req = http::Request {
        method: http::Method::Get,
        url,
        headers,
        body: String::default(),
    };

    // Execute the HTTP request and handle the response
    let resp = http::get(&req).map_err(|e| format!("HTTP request failed: {}", e))?;

    // Parse the response body as JSON
    let resp_json: JsonValue = serde_json::from_str(&resp.body)
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    // Ensure the response contains an array of customers
    this.src_rows = resp_json
        .get("customers")
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or("Expected 'customers' field with an array in the response")?;

    // Log the number of customers retrieved
    utils::report_info(&format!("Retrieved {} customers from Square API", this.src_rows.len()));

    Ok(())
}

    
    
    

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();
    
        // Check if we've reached the end of the response array
        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }
    
        // Retrieve the current customer row
        let src_row = &this.src_rows[this.src_idx];
        
        // Iterate over the columns requested by the FDW
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            
            // Handle potential nested structure in the Square API response
            let src = src_row
                .as_object()
                .and_then(|v| v.get(&tgt_col_name)) // Extract the specific field for the target column
                .ok_or(format!("source column '{}' not found", tgt_col_name))?;
    
            // Match the target column's data type with the source data type
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
                    ));
                }
            };
    
            // Add the processed data to the row
            row.push(cell.as_ref());
        }
    
        // Increment the index for the next row in the response
        this.src_idx += 1;
    
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
