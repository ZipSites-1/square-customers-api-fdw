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
        // semver expression for Wasm FDW host version requirement
        // ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://connect.squareup.com/v2/customers");

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        let opts = ctx.get_options(OptionsType::Table);
        let object = opts.require("object")?;
        let limit = opts.get("limit").unwrap_or("100").to_string();  // Set default limit to 100
        let mut cursor = opts.get("cursor").unwrap_or("").to_string();  // Handle pagination cursor

        let mut url = format!("{}?limit={}", this.base_url, limit);
        if !cursor.is_empty() {
             url = format!("{}&cursor={}", url, cursor);
        }

        let access_token = opts.require("access_token")?;  // Fetch access token from options
        let headers: Vec<(String, String)> = vec![
            ("Authorization".to_owned(), format!("Bearer {}", access_token)),
            ("Content-Type".to_owned(), "application/json".to_owned()),
        ];

        let req = http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        };
        let resp = http::get(&req)?;
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        this.src_rows = resp_json["customers"]
            .as_array()
            .map(|v| v.to_owned())
            .expect("response should be a JSON array");


        utils::report_info(&format!("We got response array length: {}", this.src_rows.len()));

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // If all source rows are consumed, check if there's a next page to fetch
        if this.src_idx >= this.src_rows.len() {
            if let Some(ref cursor) = this.next_cursor {
                // Build the next page request if there's a cursor
                let mut url = format!("{}?limit={}", this.base_url, this.limit);
                if !cursor.is_empty() {
                    url = format!("{}&cursor={}", url, cursor);
                }

                // Set request headers
                let headers: Vec<(String, String)> = vec![
                    ("Authorization".to_owned(), format!("Bearer {}", this.access_token)),
                    ("Content-Type".to_owned(), "application/json".to_owned()),
                ];

                // Make the next page request
                let req = http::Request {
                    method: http::Method::Get,
                    url,
                    headers,
                    body: String::default(),
                };
                let resp = http::get(&req)?;
                let body = resp.body;

                // Parse response and update rows and cursor
                let json_response: JsonValue = serde_json::from_str(&body).map_err(|e| e.to_string())?;
                this.src_rows = json_response["customers"].as_array().unwrap().clone();
                this.next_cursor = json_response.get("cursor").map(|c| c.as_str().unwrap().to_string());
                
                // Reset index for the new page
                this.src_idx = 0;
            } else {
                // No more pages to fetch
                return Ok(None);
            }
        }

        // Process the current row
        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            let src = src_row
                .as_object()
                .and_then(|v| v.get(tgt_col_name))
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
                        "column {} data type is not supported",
                        tgt_col_name
                    ));
                }
            };

            row.push(cell.as_ref());
        }

        // Move to the next row in the current page
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

// #[allow(warnings)]
// mod bindings;
// use serde_json::Value as JsonValue;

// use bindings::{
//     exports::supabase::wrappers::routines::Guest,
//     supabase::wrappers::{
//         http, time,
//         types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
//         utils,
//     },
// };

// #[derive(Debug, Default)]
// struct ExampleFdw {
//     base_url: String,
//     src_rows: Vec<JsonValue>,
//     src_idx: usize,
// }

// // pointer for the static FDW instance
// static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();

// impl ExampleFdw {
//     // initialise FDW instance
//     fn init_instance() {
//         let instance = Self::default();
//         unsafe {
//             INSTANCE = Box::leak(Box::new(instance));
//         }
//     }

//     fn this_mut() -> &'static mut Self {
//         unsafe { &mut (*INSTANCE) }
//     }
// }

// impl Guest for ExampleFdw {
//     fn host_version_requirement() -> String {
//         // semver expression for Wasm FDW host version requirement
//         // ref: https://docs.rs/semver/latest/semver/enum.Op.html
//         "^0.1.0".to_string()
//     }

//     fn init(ctx: &Context) -> FdwResult {
//         Self::init_instance();
//         let this = Self::this_mut();

//         let opts = ctx.get_options(OptionsType::Server);
//         this.base_url = opts.require_or("api_url", "https://api.github.com");

//         Ok(())
//     }

//     fn begin_scan(ctx: &Context) -> FdwResult {
//         let this = Self::this_mut();

//         let opts = ctx.get_options(OptionsType::Table);
//         let object = opts.require("object")?;
//         let url = format!("{}/{}", this.base_url, object);

//         let headers: Vec<(String, String)> =
//             vec![("user-agent".to_owned(), "Example FDW".to_owned())];

//         let req = http::Request {
//             method: http::Method::Get,
//             url,
//             headers,
//             body: String::default(),
//         };
//         let resp = http::get(&req)?;
//         let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

//         this.src_rows = resp_json
//             .as_array()
//             .map(|v| v.to_owned())
//             .expect("response should be a JSON array");

//         utils::report_info(&format!("We got response array length: {}", this.src_rows.len()));

//         Ok(())
//     }

//     fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
//         let this = Self::this_mut();

//         if this.src_idx >= this.src_rows.len() {
//             return Ok(None);
//         }

//         let src_row = &this.src_rows[this.src_idx];
//         for tgt_col in ctx.get_columns() {
//             let tgt_col_name = tgt_col.name();
//             let src = src_row
//                 .as_object()
//                 .and_then(|v| v.get(&tgt_col_name))
//                 .ok_or(format!("source column '{}' not found", tgt_col_name))?;
//             let cell = match tgt_col.type_oid() {
//                 TypeOid::Bool => src.as_bool().map(Cell::Bool),
//                 TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
//                 TypeOid::Timestamp => {
//                     if let Some(s) = src.as_str() {
//                         let ts = time::parse_from_rfc3339(s)?;
//                         Some(Cell::Timestamp(ts))
//                     } else {
//                         None
//                     }
//                 }
//                 TypeOid::Json => src.as_object().map(|_| Cell::Json(src.to_string())),
//                 _ => {
//                     return Err(format!(
//                         "column {} data type is not supported",
//                         tgt_col_name
//                     ));
//                 }
//             };

//             row.push(cell.as_ref());
//         }

//         this.src_idx += 1;

//         Ok(Some(0))
//     }

//     fn re_scan(_ctx: &Context) -> FdwResult {
//         Err("re_scan on foreign table is not supported".to_owned())
//     }

//     fn end_scan(_ctx: &Context) -> FdwResult {
//         let this = Self::this_mut();
//         this.src_rows.clear();
//         Ok(())
//     }

//     fn begin_modify(_ctx: &Context) -> FdwResult {
//         Err("modify on foreign table is not supported".to_owned())
//     }

//     fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
//         Ok(())
//     }

//     fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
//         Ok(())
//     }

//     fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
//         Ok(())
//     }

//     fn end_modify(_ctx: &Context) -> FdwResult {
//         Ok(())
//     }
// }

// bindings::export!(ExampleFdw with_types_in bindings);
