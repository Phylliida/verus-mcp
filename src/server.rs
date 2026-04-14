use crate::editor;
use crate::index::{Index, Matcher, DEFAULT_RESULTS, MAX_RESULTS};
use crate::indexer;
use crate::params_de;
use crate::types::FnKind;
use rmcp::{
    handler::server::{router::tool::ToolRouter, tool::ToolCallContext, wrapper::Parameters},
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    tool, tool_router, ErrorData as McpError, RoleServer, ServerHandler,
};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

///  JSON diagnostic span from `--message-format=json` output.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct DiagSpan {
    file_name: String,
    line_start: usize,
    line_end: usize,
    column_start: usize,
    column_end: usize,
    is_primary: bool,
    label: Option<String>,
}

///  JSON diagnostic from `--message-format=json` output.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct DiagMessage {
    message: String,
    level: String,
    spans: Vec<DiagSpan>,
    rendered: Option<String>,
    code: Option<DiagCode>,
    children: Option<Vec<DiagMessage>>,
}

///  Optional error code (e.g., E0308).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct DiagCode {
    code: String,
}

///  Cargo JSON line with `reason` field.
#[derive(Debug, Deserialize)]
struct CargoJsonLine {
    reason: String,
    message: Option<DiagMessage>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchParams {
    ///  Name substring to search for
    pub query: String,
    ///  Filter by function kind: "spec", "proof", or "exec"
    pub kind: Option<String>,
    ///  Filter by crate name
    pub crate_name: Option<String>,
    ///  Filter by module path substring
    pub module: Option<String>,
    ///  Only show trait axioms/methods
    #[serde(default, deserialize_with = "params_de::bool_default")]
    pub trait_only: bool,
    ///  When true, return full signatures with requires/ensures (default limit drops to 10)
    #[serde(default, deserialize_with = "params_de::bool_default")]
    pub details: bool,
    ///  Max results to return (default 50, or 10 when details=true)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub limit: Option<usize>,
    ///  Skip first N results for pagination (default 0)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LookupParams {
    ///  Exact function name to look up
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct BatchLookupParams {
    ///  List of function/type names to look up (max 10)
    #[serde(deserialize_with = "params_de::vec_string")]
    pub names: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ClauseSearchParams {
    ///  Substring to search within requires/ensures clauses
    pub query: String,
    ///  Filter by crate name
    pub crate_name: Option<String>,
    ///  Filter by module path substring
    pub module: Option<String>,
    ///  Filter by function name substring
    pub name: Option<String>,
    ///  Filter by function kind: "spec", "proof", or "exec"
    pub kind: Option<String>,
    ///  Max results to return (default 50)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub limit: Option<usize>,
    ///  Skip first N results for pagination (default 0)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SignatureSearchParams {
    ///  Substring to match against parameter types (e.g., "Vec2", "Point3", "Seq")
    pub param_type: Option<String>,
    ///  Substring to match against return type (e.g., "bool", "Sign")
    pub return_type: Option<String>,
    ///  Substring to match against type parameter bounds (e.g., "OrderedRing", "Field")
    pub type_bound: Option<String>,
    ///  Optional name substring filter to combine with type filters
    pub name: Option<String>,
    ///  Filter by function kind: "spec", "proof", or "exec"
    pub kind: Option<String>,
    ///  Filter by crate name
    pub crate_name: Option<String>,
    ///  Filter by module path substring
    pub module: Option<String>,
    ///  Max results to return (default 50)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub limit: Option<usize>,
    ///  Skip first N results for pagination (default 0)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CheckParams {
    ///  Crate directory name (e.g., "verus-geometry", "verus-topology")
    pub crate_name: String,
    ///  Optional: verify only this module. Accepts a file path (e.g., "src/runtime/polygon.rs")
    ///  or module path (e.g., "runtime::polygon"). Bypasses check.sh and runs cargo verus directly.
    pub module: Option<String>,
    ///  When true, return raw compiler output instead of parsed diagnostics. Useful when error parsing misses something.
    #[serde(default, deserialize_with = "params_de::opt_bool")]
    pub raw: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct BuildParams {
    ///  Crate directory name (e.g., "verus-geometry")
    pub crate_name: String,
    ///  Cargo features to enable (e.g., "feat1,feat2")
    pub features: Option<String>,
    ///  Build in release mode
    #[serde(default, deserialize_with = "params_de::opt_bool")]
    pub release: Option<bool>,
    ///  Extra flags passed to cargo build (e.g., "--target x86_64-unknown-linux-gnu")
    pub extra_args: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunParams {
    ///  Crate directory name (e.g., "verus-geometry")
    pub crate_name: String,
    ///  Cargo features to enable (e.g., "feat1,feat2")
    pub features: Option<String>,
    ///  Run in release mode
    #[serde(default, deserialize_with = "params_de::opt_bool")]
    pub release: Option<bool>,
    ///  Extra flags passed to cargo run (before --)
    pub extra_args: Option<String>,
    ///  Arguments passed to the binary (after --)
    pub args: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProfileParams {
    ///  Crate directory name (e.g., "verus-geometry", "verus-topology")
    pub crate_name: String,
    ///  Optional: profile only this module. Accepts a file path or module path.
    pub module: Option<String>,
    ///  Number of top functions to show (default: 25)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub top_n: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DependencyParams {
    ///  Function name to find dependencies for
    pub name: String,
    ///  Direction: "callers" (who calls this function) or "callees" (what this function calls). Default: "callers"
    pub direction: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ContextActivateParams {
    ///  Context name to activate or create. Omit to list recent contexts.
    pub name: Option<String>,
}

//  --- Unified search tool params (standalone mode only) ---

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FindParams {
    ///  Search query — name substring, clause/body content, module path, etc.
    pub query: Option<String>,
    ///  Exact name for lookup, source view, trait search, module browsing, or dependencies
    pub name: Option<String>,
    ///  Multiple names for batch lookup (max 10)
    #[serde(default, deserialize_with = "params_de::opt_vec_string")]
    pub names: Option<Vec<String>>,
    ///  Search scope (omit for name search/lookup):
    ///  "ensures", "requires", "body", "doc" — search clause/body/doc content
    ///  "types" — search structs/enums by name
    ///  "signature" — search by param_type/return_type/type_bound
    ///  "trait" — trait definition + implementors
    ///  "module" — browse module contents
    ///  "modules" — list all modules
    ///  "dependencies" — callers/callees (set direction)
    ///  "stats" — index statistics
    ///  "source" — full source code of a function
    pub scope: Option<String>,
    ///  Filter by function kind: "spec", "proof", "exec"
    pub kind: Option<String>,
    ///  Filter by crate name
    pub crate_name: Option<String>,
    ///  Filter by module path substring
    pub module: Option<String>,
    ///  For signature search: match parameter types
    pub param_type: Option<String>,
    ///  For signature search: match return type
    pub return_type: Option<String>,
    ///  For signature search: match type parameter bounds
    pub type_bound: Option<String>,
    ///  For dependencies: "callers" (default) or "callees"
    pub direction: Option<String>,
    ///  Return full signatures with requires/ensures
    #[serde(default, deserialize_with = "params_de::bool_default")]
    pub details: bool,
    ///  Only show trait axioms/methods
    #[serde(default, deserialize_with = "params_de::bool_default")]
    pub trait_only: bool,
    ///  Max results (default 50, or 10 when details=true)
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub limit: Option<usize>,
    ///  Skip first N results for pagination
    #[serde(default, deserialize_with = "params_de::opt_usize")]
    pub offset: Option<usize>,
}

//  --- Code editing tool params (standalone mode only) ---

///  Structured function definition. Provide EITHER `source` (raw source code)
///  OR the structured fields (name, kind, params, body, etc.) — not both.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct FnSpec {
    ///  Raw source code of the function. If provided, all other fields are ignored.
    pub source: Option<String>,
    ///  Function name (required when not using raw source)
    pub name: Option<String>,
    ///  Function kind: "spec", "proof", "exec", or omit for regular fn
    pub kind: Option<String>,
    ///  Visibility: "pub", "pub(crate)", or omit for private
    pub visibility: Option<String>,
    ///  Whether this is an `open` spec fn
    #[serde(default, deserialize_with = "params_de::bool_default")]
    pub open: bool,
    ///  Generic type parameters, e.g. "<T: Ring>"
    pub type_params: Option<String>,
    ///  Parameter list including parens, e.g. "(a: nat, b: nat)"
    pub params: Option<String>,
    ///  Return type, e.g. "bool" or "(nat, nat)"
    pub return_type: Option<String>,
    ///  Requires clauses (each is one predicate)
    #[serde(default, deserialize_with = "params_de::opt_vec_string")]
    pub requires: Option<Vec<String>>,
    ///  Ensures clauses (each is one predicate)
    #[serde(default, deserialize_with = "params_de::opt_vec_string")]
    pub ensures: Option<Vec<String>>,
    ///  Decreases clause, e.g. "n"
    pub decreases: Option<String>,
    ///  Function body (content inside `{ }`). Omit for signature-only (trait methods).
    pub body: Option<String>,
    ///  Doc comment text (will be prefixed with `///` per line)
    pub doc: Option<String>,
    ///  Attributes, e.g. ["#[verifier::external_body]"]
    #[serde(default, deserialize_with = "params_de::opt_vec_string")]
    pub annotations: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadParams {
    ///  File or directory path. Omit for current directory.
    pub path: Option<String>,
    ///  Function name to read full source (requires path to be a file).
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AddParams {
    ///  Absolute path to the file
    pub file: String,
    ///  Use path to add (e.g., "vstd::prelude::*" or short name like "Ring" for auto-resolve)
    pub use_path: Option<String>,
    ///  Module name to add as `pub mod <name>;`
    pub mod_name: Option<String>,
    ///  Function definition (structured or raw source) — used when use_path and mod_name are both absent
    #[serde(flatten)]
    pub spec: FnSpec,
    ///  Insert after this function name (otherwise appends)
    pub after: Option<String>,
    ///  Trait or impl name to insert the method into (e.g., "MinimalPoly" or "Ring for SpecFieldExt")
    pub inside: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RemoveParams {
    ///  Absolute path to the file
    pub file: String,
    ///  Function name to remove (or "Type::method" for impl methods)
    pub name: Option<String>,
    ///  Use path substring to match and remove
    pub use_path: Option<String>,
    ///  Module name to remove (removes `pub mod <name>;` or `mod <name>;`)
    pub mod_name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct EditParams {
    ///  Absolute path to the file
    pub file: String,
    ///  Function name (or "Type::method") to scope the edit. Omit to edit use statements.
    pub name: Option<String>,
    ///  Exact string to find within the function (must be unique within it)
    pub old_string: String,
    ///  Replacement string
    pub new_string: String,
}

struct ContextState {
    active: Option<String>,
    items: Vec<String>,
    listed: bool,
}

impl ContextState {
    fn new() -> Self {
        ContextState {
            active: None,
            items: Vec::new(),
            listed: false,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ContextFile {
    name: String,
    created: u64,
    last_used: u64,
    items: Vec<String>,
}

fn contexts_dir() -> std::path::PathBuf {
    crate::indexer::find_workspace_root()
        .join(".verus-mcp")
        .join("contexts")
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn format_relative_time(timestamp: u64) -> String {
    let now = now_unix();
    let diff = now.saturating_sub(timestamp);
    if diff < 60 { return "just now".to_string(); }
    if diff < 3600 { return format!("{}m ago", diff / 60); }
    if diff < 86400 { return format!("{}h ago", diff / 3600); }
    if diff < 604800 { return format!("{}d ago", diff / 86400); }
    format!("{}w ago", diff / 604800)
}

fn load_context(name: &str) -> Option<ContextFile> {
    let path = contexts_dir().join(format!("{}.json", name));
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

fn save_context(name: &str, items: &[String]) {
    let dir = contexts_dir();
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("{}.json", name));

    let existing = load_context(name);
    let created = existing.map(|c| c.created).unwrap_or_else(now_unix);

    let cf = ContextFile {
        name: name.to_string(),
        created,
        last_used: now_unix(),
        items: items.to_vec(),
    };
    let _ = std::fs::write(&path, serde_json::to_string_pretty(&cf).unwrap_or_default());
}

fn list_contexts() -> Vec<ContextFile> {
    let dir = contexts_dir();
    let mut contexts = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Ok(data) = std::fs::read_to_string(&path) {
                    if let Ok(cf) = serde_json::from_str::<ContextFile>(&data) {
                        contexts.push(cf);
                    }
                }
            }
        }
    }
    contexts.sort_by(|a, b| b.last_used.cmp(&a.last_used));
    contexts.truncate(25);
    contexts
}

fn replay_items(idx: &Index, items: &[String]) -> String {
    let mut fn_parts = Vec::new();
    let mut type_parts = Vec::new();
    let mut not_found = Vec::new();

    for name in items {
        let fn_results = idx.lookup(name);
        if !fn_results.is_empty() {
            for e in &fn_results {
                fn_parts.push(e.format_full());
            }
            continue;
        }
        let type_results = idx.lookup_type(name);
        if !type_results.is_empty() {
            for e in &type_results {
                type_parts.push(e.format_full());
            }
            continue;
        }
        not_found.push(name.as_str());
    }

    let mut text = String::new();
    if !fn_parts.is_empty() {
        text.push_str(&fn_parts.join("\n"));
    }
    if !type_parts.is_empty() {
        if !text.is_empty() { text.push('\n'); }
        text.push_str(&type_parts.join("\n"));
    }
    if !not_found.is_empty() {
        if !text.is_empty() { text.push_str("\n\n"); }
        text.push_str("---\nNot found (may have been renamed): ");
        text.push_str(&not_found.join(", "));
    }
    text
}

///  Validate and resolve a module path for --verify-module.
///  Returns Ok(flag_string) or Err(error_message).
fn validate_module(crate_name: &str, module: &str, crate_dir: &std::path::Path) -> Result<String, String> {
    let trimmed = module.trim();
    if trimmed.contains('>') || trimmed.is_empty() {
        return Err(format!(
            "Error: invalid module path '{}'. Use a file path like 'src/foo.rs' or module path like 'foo::bar'.",
            module
        ));
    }

    //  If it looks like a file path, check it exists
    if trimmed.contains('/') || trimmed.ends_with(".rs") {
        let file_path = if trimmed.starts_with("src/") {
            crate_dir.join(trimmed)
        } else {
            crate_dir.join("src").join(trimmed)
        };
        if !file_path.exists() {
            //  Collect all .rs files for fuzzy suggestions
            let src_dir = crate_dir.join("src");
            let mut rs_files = Vec::new();
            if let Ok(entries) = walkdir_rs_files(&src_dir) {
                rs_files = entries;
            }

            let needle = trimmed.strip_prefix("src/").unwrap_or(trimmed);
            let mut suggestions: Vec<(usize, &str)> = rs_files.iter()
                .map(|f| (strsim_distance(needle, f), f.as_str()))
                .filter(|(d, _)| *d <= 5)
                .collect();
            suggestions.sort_by_key(|(d, _)| *d);
            suggestions.truncate(3);

            let hint = if suggestions.is_empty() {
                String::new()
            } else {
                format!("\n\nDid you mean: {}",
                    suggestions.iter().map(|(_, f)| format!("src/{}", f)).collect::<Vec<_>>().join(", "))
            };
            return Err(format!(
                "Error: module file '{}' not found in {}/src/.{}",
                trimmed, crate_name, hint
            ));
        }
    }

    Ok(format!("--verify-module {} ", to_verify_module(crate_name, trimmed)))
}

///  Walk src/ directory for .rs files, returning paths relative to src/.
fn walkdir_rs_files(src_dir: &std::path::Path) -> Result<Vec<String>, std::io::Error> {
    let mut files = Vec::new();
    fn walk(dir: &std::path::Path, prefix: &str, files: &mut Vec<String>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().to_string();
                if path.is_dir() {
                    let sub = if prefix.is_empty() { name.clone() } else { format!("{}/{}", prefix, name) };
                    walk(&path, &sub, files);
                } else if name.ends_with(".rs") {
                    let rel = if prefix.is_empty() { name } else { format!("{}/{}", prefix, name) };
                    files.push(rel);
                }
            }
        }
    }
    walk(src_dir, "", &mut files);
    Ok(files)
}

///  Simple Levenshtein distance for short strings.
fn strsim_distance(a: &str, b: &str) -> usize {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let (m, n) = (a.len(), b.len());
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0; n + 1];
    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

///  Convert a file path or module path to a Verus --verify-module argument.
///  Verus uses crate-local module paths (e.g., "runtime::polygon", not "verus_geometry::runtime::polygon").
///  Accepts: "src/runtime/polygon.rs", "runtime/polygon.rs", "runtime::polygon"
fn to_verify_module(crate_name: &str, input: &str) -> String {
    let crate_mod = crate_name.replace('-', "_");

    //  If it already looks like a module path (has :: and no /), strip crate prefix if present
    if input.contains("::") && !input.contains('/') {
        let stripped = input
            .strip_prefix(&format!("{}::", crate_mod))
            .or_else(|| input.strip_prefix("crate::"))
            .unwrap_or(input);
        return stripped.to_string();
    }

    //  File path: strip src/ prefix and .rs suffix, convert / to ::
    let rel = input.strip_prefix("src/").unwrap_or(input);
    let rel = rel.strip_suffix(".rs").unwrap_or(rel);
    let module = rel
        .replace('/', "::")
        .replace("::mod", "");

    if module.is_empty() || module == "lib" || module == "main" {
        crate_mod
    } else {
        module
    }
}

///  If the body field contains a full function (with signature, requires/ensures),
///  assemble the structured fields into a signature and wrap the body to form a
///  complete function, then parse with tree-sitter. If it parses as a valid function,
///  use it as raw source. This handles models that dump everything into the body field.
fn normalize_fn_spec(spec: &FnSpec) -> FnSpec {
    let body = match spec.body {
        Some(ref b) => b,
        None => return spec.clone(),
    };

    //  Quick check: does the body contain function-like keywords at the start?
    let trimmed = body.trim_start();
    let has_fn_sig = trimmed.contains("fn ");
    let has_clauses = trimmed.starts_with("requires") || trimmed.starts_with("ensures");

    if !has_fn_sig && !has_clauses {
        return spec.clone();
    }

    //  Strategy: assemble a candidate function from the structured fields + body,
    //  wrapping body in verus! { } so tree-sitter can parse it.
    //  If body contains a full function, try parsing it directly first.
    if has_fn_sig {
        //  Body might be an entire function definition — try parsing it directly
        let candidate = format!("verus! {{\n{}\n}}", body);
        if let Ok(items) = editor::parse_file(&candidate) {
            if !items.functions.is_empty() {
                let f = &items.functions[0];
                let parsed_source = candidate[f.start_byte..f.end_byte].to_string();
                let mut normalized = spec.clone();
                normalized.source = Some(parsed_source);
                normalized.body = None;
                return normalized;
            }
        }
    }

    //  Body starts with requires/ensures but no fn signature —
    //  build the signature from structured fields, prepend it, then parse
    if has_clauses {
        let mut sig = String::new();
        if let Some(ref vis) = spec.visibility {
            sig.push_str(vis);
            sig.push(' ');
        }
        if spec.open {
            sig.push_str("open ");
        }
        if let Some(ref kind) = spec.kind {
            sig.push_str(kind);
            sig.push_str(" fn ");
        } else {
            sig.push_str("fn ");
        }
        sig.push_str(spec.name.as_deref().unwrap_or("__placeholder"));
        if let Some(ref tp) = spec.type_params {
            sig.push_str(tp);
        }
        sig.push_str(spec.params.as_deref().unwrap_or("()"));
        if let Some(ref ret) = spec.return_type {
            sig.push_str(" -> ");
            sig.push_str(ret);
        }
        //  Body has requires/ensures — check if it also has a { } block at the end
        let candidate = format!("verus! {{\n{}\n{}\n}}", sig, body);
        if let Ok(items) = editor::parse_file(&candidate) {
            if !items.functions.is_empty() {
                let f = &items.functions[0];
                let parsed_source = candidate[f.start_byte..f.end_byte].to_string();
                let mut normalized = spec.clone();
                normalized.source = Some(parsed_source);
                normalized.body = None;
                return normalized;
            }
        }
    }

    spec.clone()
}

///  Assemble function source code from a FnSpec. Returns the raw `source` if
///  provided, otherwise builds it from structured fields.
fn assemble_fn(spec: &FnSpec) -> Result<String, String> {
    let spec = &normalize_fn_spec(spec);

    //  Raw source shortcut
    if let Some(ref src) = spec.source {
        return Ok(src.clone());
    }

    let name = spec.name.as_deref().ok_or("Function name is required when not using raw source")?;

    let mut out = String::new();

    //  Doc comment
    if let Some(ref doc) = spec.doc {
        for line in doc.lines() {
            out.push_str(&format!("///  {}\n", line));
        }
    }

    //  Annotations
    if let Some(ref annotations) = spec.annotations {
        for ann in annotations {
            if ann.starts_with("#[") {
                out.push_str(ann);
            } else {
                out.push_str(&format!("#[{}]", ann));
            }
            out.push('\n');
        }
    }

    //  Signature line: [vis] [open] [kind] fn name[type_params](params) [-> ret]
    let mut sig = String::new();
    if let Some(ref vis) = spec.visibility {
        sig.push_str(vis);
        sig.push(' ');
    }
    if spec.open {
        sig.push_str("open ");
    }
    if let Some(ref kind) = spec.kind {
        sig.push_str(kind);
        sig.push_str(" fn ");
    } else {
        sig.push_str("fn ");
    }
    sig.push_str(name);
    if let Some(ref tp) = spec.type_params {
        sig.push_str(tp);
    }
    sig.push_str(spec.params.as_deref().unwrap_or("()"));
    if let Some(ref ret) = spec.return_type {
        sig.push_str(" -> ");
        sig.push_str(ret);
    }
    out.push_str(&sig);

    //  Requires
    if let Some(ref reqs) = spec.requires {
        if !reqs.is_empty() {
            out.push_str("\n    requires\n");
            for (i, r) in reqs.iter().enumerate() {
                out.push_str("        ");
                out.push_str(r);
                if i + 1 < reqs.len() {
                    out.push(',');
                }
                out.push('\n');
            }
        }
    }

    //  Ensures
    if let Some(ref enss) = spec.ensures {
        if !enss.is_empty() {
            out.push_str("    ensures\n");
            for (i, e) in enss.iter().enumerate() {
                out.push_str("        ");
                out.push_str(e);
                if i + 1 < enss.len() {
                    out.push(',');
                }
                out.push('\n');
            }
        }
    }

    //  Decreases
    if let Some(ref dec) = spec.decreases {
        out.push_str(&format!("    decreases {},\n", dec));
    }

    //  Body
    if let Some(ref body) = spec.body {
        out.push_str("{\n");
        for line in body.lines() {
            out.push_str("    ");
            out.push_str(line);
            out.push('\n');
        }
        out.push('}');
    }

    Ok(out)
}

///  Run a bash script with a 10-minute timeout. Returns the process output.
async fn run_bash_script(
    script: &str,
    crate_dir: &std::path::Path,
) -> Result<std::process::Output, String> {
    use tokio::process::Command;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(600),
        Command::new("bash")
            .arg("-c")
            .arg(script)
            .current_dir(crate_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output(),
    )
    .await;
    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(format!("Failed to run cargo verus: {}", e)),
        Err(_) => Err("cargo verus timed out after 10 minutes".to_string()),
    }
}

///  Build the bash script for `cargo verus verify` (check mode).
///
///  When `use_json` is true, adds `--message-format=json` for structured diagnostics.
///  When false (raw mode), omits it so output is human-readable.
///
///  When `--verify-module` is used, a silent pre-build caches dependencies first.
///  All pre-build output is suppressed to avoid leaking stale verification counts.
fn build_check_script(
    default_verus_root: &std::path::Path,
    pkg: &str,
    module_flag: &str,
    use_json: bool,
) -> String {
    //  When --verify-module is used, we must pre-build dependencies without the flag.
    //  Flags after `--` are passed to ALL rustc invocations (including deps), so
    //  --verify-module would cause Verus to fail on dependency crates that don't have
    //  the specified module. Pre-building caches deps so the real verify only compiles
    //  the target crate.
    //
    //  All pre-build output (stdout+stderr) is suppressed to prevent its verification
    //  summary from leaking into the parsed output.
    let prebuild = if module_flag.contains("--verify-module") {
        format!(
            "cargo verus verify --manifest-path Cargo.toml -p {pkg} -- --no-verify --triggers-mode silent >/dev/null 2>&1 || true\n",
            pkg = pkg,
        )
    } else {
        String::new()
    };
    let json_flag = if use_json { "--message-format=json " } else { "" };
    format!(
        r#"set -euo pipefail
VERUS_ROOT="${{VERUS_ROOT:-{default_verus_root}}}"
VERUS_SOURCE="$VERUS_ROOT/source"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TOOLCHAIN="1.94.0-aarch64-apple-darwin" ;;
  Darwin-x86_64) TOOLCHAIN="1.94.0-x86_64-apple-darwin" ;;
  *)             TOOLCHAIN="1.94.0-x86_64-unknown-linux-gnu" ;;
esac
export PATH="$VERUS_SOURCE/target-verus/release:$PATH"
export VERUS_Z3_PATH="$VERUS_SOURCE/z3"
export RUSTUP_TOOLCHAIN="$TOOLCHAIN"
{prebuild}cargo verus verify --manifest-path Cargo.toml -p {pkg} {json_flag}-- {module_flag}-V cache --triggers-mode silent || true
"#,
        default_verus_root = default_verus_root.display(),
        pkg = pkg,
        module_flag = module_flag,
        prebuild = prebuild,
        json_flag = json_flag,
    )
}

///  Check if dependency crates were compiled during a cargo verus run.
///  Looks for "Compiling <crate>" lines in stderr where <crate> is not the target.
fn has_dependency_compilation(stderr: &str, target_crate: &str) -> bool {
    let target_underscore = target_crate.replace('-', "_");
    for line in stderr.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Compiling ") {
            let crate_name = rest.split_whitespace().next().unwrap_or("");
            if crate_name != target_crate && crate_name != target_underscore {
                return true;
            }
        }
    }
    false
}

///  Pick the verus verification summary from combined stdout+stderr output.
///
///  Returns `(verified, errors, cached)` for the most informative summary line,
///  or `None` if no summary line is present.
///
///  Full-crate `cargo verus verify` can emit TWO summary lines for the target:
///  one from the verify pass with real counts (including `cached`), and a
///  trailing one from a follow-up build pass that prints `0 verified, 0 errors`.
///  We pick the summary with the largest total work (verified + errors + cached)
///  so the empty trailing summary never wins.
///
///  Dependency summaries from a previous run are filtered upstream by
///  `has_dependency_compilation` triggering a rerun, so by the time this is
///  called only target-crate summaries should remain.
fn pick_verus_summary(combined: &str) -> Option<(usize, usize, usize)> {
    let re = regex::Regex::new(
        r"verification results::\s*(\d+) verified,\s*(\d+) errors(?:,\s*(\d+) cached)?",
    )
    .unwrap();
    re.captures_iter(combined)
        .map(|c| {
            let v: usize = c[1].parse().unwrap_or(0);
            let e: usize = c[2].parse().unwrap_or(0);
            let cached: usize =
                c.get(3).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
            (v, e, cached)
        })
        .max_by_key(|(v, e, c)| v + e + c)
}

///  Format a resolved one-liner showing the effective command with env vars.
fn format_resolved_build_command(
    default_verus_root: &std::path::Path,
    pkg: &str,
    features: Option<&str>,
    release: bool,
    extra_args: Option<&str>,
) -> String {
    let verus_root = std::env::var("VERUS_ROOT")
        .unwrap_or_else(|_| default_verus_root.display().to_string());
    let source = format!("{}/source", verus_root);
    let cargo_verus = format!("{}/target-verus/release/cargo-verus", source);
    let z3_path = format!("{}/z3", source);
    let toolchain = if cfg!(target_os = "macos") {
        if cfg!(target_arch = "aarch64") {
            "1.94.0-aarch64-apple-darwin"
        } else {
            "1.94.0-x86_64-apple-darwin"
        }
    } else {
        "1.94.0-x86_64-unknown-linux-gnu"
    };

    let mut cmd = format!(
        "VERUS_Z3_PATH={} RUSTUP_TOOLCHAIN={} {} build --manifest-path Cargo.toml -p {}",
        z3_path, toolchain, cargo_verus, pkg,
    );
    if let Some(f) = features {
        cmd.push_str(&format!(" --features {}", f));
    }
    if release {
        cmd.push_str(" --release");
    }
    if let Some(extra) = extra_args {
        if !extra.is_empty() {
            cmd.push_str(&format!(" {}", extra));
        }
    }
    cmd
}

///  Build the bash script for `cargo-verus build`.
fn build_build_script(
    default_verus_root: &std::path::Path,
    pkg: &str,
    features: Option<&str>,
    release: bool,
    extra_args: Option<&str>,
) -> String {
    let features_flag = features.map(|f| format!("--features {} ", f)).unwrap_or_default();
    let release_flag = if release { "--release " } else { "" };
    let extra = extra_args.unwrap_or("");
    format!(
        r#"set -euo pipefail
VERUS_ROOT="${{VERUS_ROOT:-{default_verus_root}}}"
VERUS_SOURCE="$VERUS_ROOT/source"
CARGO_VERUS="$VERUS_SOURCE/target-verus/release/cargo-verus"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TOOLCHAIN="1.94.0-aarch64-apple-darwin" ;;
  Darwin-x86_64) TOOLCHAIN="1.94.0-x86_64-apple-darwin" ;;
  *)             TOOLCHAIN="1.94.0-x86_64-unknown-linux-gnu" ;;
esac
export PATH="$VERUS_SOURCE/target-verus/release:$PATH"
export VERUS_Z3_PATH="$VERUS_SOURCE/z3"
export RUSTUP_TOOLCHAIN="$TOOLCHAIN"
"$CARGO_VERUS" build --manifest-path Cargo.toml -p {pkg} {features_flag}{release_flag}{extra}--message-format=json -- -V cache --triggers-mode silent 2>&1 || true
"#,
        default_verus_root = default_verus_root.display(),
        pkg = pkg,
        features_flag = features_flag,
        release_flag = release_flag,
        extra = if extra.is_empty() { String::new() } else { format!("{} ", extra) },
    )
}

///  Build the bash script for `cargo-verus run`.
///
///  cargo-verus doesn't have a `run` subcommand, so we build first with
///  `cargo-verus build`, then run the resulting binary directly.
fn build_run_script(
    default_verus_root: &std::path::Path,
    pkg: &str,
    features: Option<&str>,
    release: bool,
    extra_args: Option<&str>,
    args: Option<&str>,
) -> String {
    let features_flag = features.map(|f| format!("--features {} ", f)).unwrap_or_default();
    let release_flag = if release { "--release " } else { "" };
    let extra = extra_args.unwrap_or("");
    let bin_args = args.unwrap_or("");
    let profile_dir = if release { "release" } else { "debug" };
    format!(
        r#"set -euo pipefail
VERUS_ROOT="${{VERUS_ROOT:-{default_verus_root}}}"
VERUS_SOURCE="$VERUS_ROOT/source"
CARGO_VERUS="$VERUS_SOURCE/target-verus/release/cargo-verus"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TOOLCHAIN="1.94.0-aarch64-apple-darwin" ;;
  Darwin-x86_64) TOOLCHAIN="1.94.0-x86_64-apple-darwin" ;;
  *)             TOOLCHAIN="1.94.0-x86_64-unknown-linux-gnu" ;;
esac
export PATH="$VERUS_SOURCE/target-verus/release:$PATH"
export VERUS_Z3_PATH="$VERUS_SOURCE/z3"
export RUSTUP_TOOLCHAIN="$TOOLCHAIN"
# Discover binary name before building
# Priority: 1) --bin flag from extra_args, 2) auto-discover from src/bin/, 3) package name
BIN_NAME=""
BIN_FLAG=""
for arg in {extra}; do
  if [ "${{prev:-}}" = "--bin" ]; then BIN_NAME="$arg"; fi
  prev="$arg"
done
if [ -z "$BIN_NAME" ] && [ -d src/bin ]; then
  for f in src/bin/*.rs; do
    BIN_NAME=$(basename "$f" .rs)
    BIN_FLAG="--bin $BIN_NAME"
    break
  done
fi
if [ -z "$BIN_NAME" ]; then
  BIN_NAME="{pkg}"
fi
"$CARGO_VERUS" build --manifest-path Cargo.toml -p {pkg} {features_flag}{release_flag}$BIN_FLAG {extra}-- -V cache --triggers-mode silent 2>&1
BIN_NAME_US=$(echo "$BIN_NAME" | tr '-' '_')
BIN_PATH="target/{profile_dir}/$BIN_NAME_US"
if [ ! -f "$BIN_PATH" ]; then
  BIN_PATH="target/{profile_dir}/$BIN_NAME"
fi
if [ ! -f "$BIN_PATH" ]; then
  echo "Binary not found. Available in target/{profile_dir}/:"
  ls target/{profile_dir}/ | head -20
  exit 1
fi
# On macOS with Nix, set up Vulkan/MoltenVK paths
if [ "$(uname -s)" = "Darwin" ]; then
  VK_LIB=$(find /nix/store -name "libvulkan.dylib" 2>/dev/null | head -1)
  if [ -n "$VK_LIB" ]; then
    export DYLD_LIBRARY_PATH="$(dirname "$VK_LIB")${{DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}}"
  fi
  VK_ICD=$(find /nix/store -name "MoltenVK_icd.json" 2>/dev/null | head -1)
  if [ -n "$VK_ICD" ]; then
    export VK_DRIVER_FILES="$VK_ICD"
  fi
fi
exec "$BIN_PATH" {bin_args} 2>&1
"#,
        default_verus_root = default_verus_root.display(),
        pkg = pkg,
        features_flag = features_flag,
        release_flag = release_flag,
        profile_dir = profile_dir,
        extra = if extra.is_empty() { String::new() } else { format!("{} ", extra) },
        bin_args = bin_args,
    )
}

///  Build the bash preamble for `cargo verus verify` (profile mode).
///  Returns everything up to (and including) the `python3 ... <<'PYEOF'` line.
fn build_profile_preamble(
    default_verus_root: &std::path::Path,
    pkg: &str,
    module_flag: &str,
    top_n: usize,
) -> String {
    //  Pre-build deps when --verify-module is used (same reason as build_check_script).
    let prebuild = if module_flag.contains("--verify-module") {
        format!(
            "cargo verus verify --manifest-path Cargo.toml -p {pkg} -- --no-verify --triggers-mode silent >/dev/null 2>&1 || true\n\n",
            pkg = pkg,
        )
    } else {
        String::new()
    };
    format!(
        r#"set -euo pipefail
unset RUSTFLAGS
unset CARGO_ENCODED_RUSTFLAGS
VERUS_ROOT="${{VERUS_ROOT:-{default_verus_root}}}"
VERUS_SOURCE="$VERUS_ROOT/source"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TOOLCHAIN="1.94.0-aarch64-apple-darwin" ;;
  Darwin-x86_64) TOOLCHAIN="1.94.0-x86_64-apple-darwin" ;;
  *)             TOOLCHAIN="1.94.0-x86_64-unknown-linux-gnu" ;;
esac
export PATH="$VERUS_SOURCE/target-verus/release:$PATH"
export VERUS_Z3_PATH="$VERUS_SOURCE/z3"
export RUSTUP_TOOLCHAIN="$TOOLCHAIN"

JSON_FILE="$(mktemp)"
trap 'rm -f "$JSON_FILE"' EXIT

{prebuild}cargo verus verify --manifest-path Cargo.toml -p {pkg} \
  -- {module_flag}-V cache --output-json --time-expanded --triggers-mode silent > "$JSON_FILE" || true

python3 - "$JSON_FILE" "{top_n}" <<'PYEOF'
"#,
        default_verus_root = default_verus_root.display(),
        pkg = pkg,
        module_flag = module_flag,
        prebuild = prebuild,
        top_n = top_n,
    )
}

///  Format "Did you mean:" suggestions, or empty string if none found.
fn format_did_you_mean(idx: &Index, query: &str) -> String {
    let suggestions = idx.suggest(query, 10);
    if suggestions.is_empty() {
        return String::new();
    }
    let mut text = String::from("\n\nDid you mean:");
    for s in &suggestions {
        text.push_str(&format!(
            "\n  {} {}  ({})  {}",
            s.label, s.name, s.location, s.module_path
        ));
    }
    text
}

fn parse_kind(s: &str) -> Option<FnKind> {
    match s.to_lowercase().as_str() {
        "spec" => Some(FnKind::Spec),
        "proof" => Some(FnKind::Proof),
        "exec" => Some(FnKind::Exec),
        _ => None,
    }
}

///  Format a result count line with pagination info.
///  "5 results", "5 of 23 results", or "results 51-75 of 100".
fn format_count(shown: usize, total: usize, offset: usize) -> String {
    if offset == 0 {
        if shown < total {
            format!("{} of {} results", shown, total)
        } else {
            format!("{} results", shown)
        }
    } else {
        let start = offset + 1;
        let end = offset + shown;
        format!("results {}-{} of {}", start, end, total)
    }
}

#[derive(Clone)]
pub struct VerusMcpServer {
    index: Arc<RwLock<Index>>,
    ready: watch::Receiver<bool>,
    context: Arc<Mutex<ContextState>>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl VerusMcpServer {
    pub fn new(index: Arc<RwLock<Index>>, ready: watch::Receiver<bool>) -> Self {
        Self {
            index,
            ready,
            context: Arc::new(Mutex::new(ContextState::new())),
            tool_router: Self::tool_router(),
        }
    }

    ///  Wait for the initial index build to complete (no-op once ready).
    async fn wait_ready(&self) {
        let mut rx = self.ready.clone();
        //  wait_for returns immediately if the value already satisfies the predicate
        let _ = rx.wait_for(|&v| v).await;
    }

    ///  Check if a context is active. Returns a gate message if not.
    ///  In standalone mode, always returns None (no context required).
    fn require_context(&self) -> Option<String> {
        if crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        let ctx = self.context.lock().unwrap();
        if ctx.active.is_some() { return None; }

        let recent = list_contexts();
        let mut msg = String::from("No context active. Activate or create one first.");
        if !recent.is_empty() {
            msg.push_str("\n\nRecent contexts:");
            for c in &recent {
                msg.push_str(&format!(
                    "\n  {} ({} items, {})",
                    c.name, c.items.len(), format_relative_time(c.last_used)
                ));
            }
        }
        msg.push_str("\n\nUse context_list to see contexts, then context_activate(name) to resume or create one.");
        Some(msg)
    }

    ///  Capture item names into the active context (no-op if no context active).
    ///  Duplicates are moved to the end (most recently fetched last).
    ///  In standalone mode, this is a complete no-op.
    fn capture_names(&self, names: impl IntoIterator<Item = impl AsRef<str>>) {
        if crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed) { return; }
        let mut ctx = self.context.lock().unwrap();
        if ctx.active.is_none() { return; }
        let mut changed = false;
        for name in names {
            let name = name.as_ref();
            if let Some(pos) = ctx.items.iter().position(|n| n == name) {
                ctx.items.remove(pos);
                ctx.items.push(name.to_string());
                changed = true;
            } else {
                ctx.items.push(name.to_string());
                changed = true;
            }
        }
        //  Trim to last 100 items to avoid context window limits on replay
        const MAX_CONTEXT_ITEMS: usize = 80;
        if ctx.items.len() > MAX_CONTEXT_ITEMS {
            let drain_count = ctx.items.len() - MAX_CONTEXT_ITEMS;
            ctx.items.drain(..drain_count);
            changed = true;
        }
        if changed {
            if let Some(ref active_name) = ctx.active {
                save_context(active_name, &ctx.items);
            }
        }
    }

    #[tool(description = "List recent contexts. Must be called before context_activate to see what contexts exist and avoid creating duplicates.")]
    pub async fn context_list(
        &self,
    ) -> Result<CallToolResult, McpError> {
        if crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(CallToolResult::success(vec![Content::text(
                "Context management is not available in standalone mode. All tools work directly without activating a context."
            )]));
        }
        let recent = list_contexts();
        let ctx_guard = self.context.lock().unwrap();
        let active_info = match &ctx_guard.active {
            Some(name) => format!("Active: {} ({} items)\n\n", name, ctx_guard.items.len()),
            None => String::new(),
        };
        drop(ctx_guard);

        //  Mark as listed so context_activate is unblocked
        self.context.lock().unwrap().listed = true;

        if recent.is_empty() && active_info.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No contexts found. Use context_activate(name=\"my-context\") to create one."
            )]));
        }

        let mut msg = active_info;
        if !recent.is_empty() {
            msg.push_str("Recent contexts:\n");
            for c in &recent {
                msg.push_str(&format!(
                    "  {} ({} items, {})\n",
                    c.name, c.items.len(), format_relative_time(c.last_used)
                ));
            }
        }
        msg.push_str("\nUse context_activate(name) to resume or create a context.");
        Ok(CallToolResult::success(vec![Content::text(msg)]))
    }

    #[tool(description = "Activate or create a context for tracking looked-up items across a session. You must call context_list first to see existing contexts. Call with a name to resume (replays all captured signatures) or create a new context. Always replays signatures on resume.")]
    pub async fn context_activate(
        &self,
        Parameters(params): Parameters<ContextActivateParams>,
    ) -> Result<CallToolResult, McpError> {
        if crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(CallToolResult::success(vec![Content::text(
                "Context management is not available in standalone mode. All tools work directly without activating a context."
            )]));
        }
        //  Gate: must call context_list first
        {
            let ctx = self.context.lock().unwrap();
            if !ctx.listed {
                return Ok(CallToolResult::success(vec![Content::text(
                    "You must call context_list first to see existing contexts before activating or creating one."
                )]));
            }
        }

        let name = match params.name {
            Some(n) => n,
            None => {
                return Ok(CallToolResult::success(vec![Content::text(
                    "context_activate requires a name. Call context_list first to see available contexts."
                )]));
            }
        };

        match load_context(&name) {
            Some(cf) => {
                //  Resume: load items, set active
                let items = cf.items;
                let item_count = items.len();

                self.wait_ready().await;
                let replay_text = {
                    let idx = self.index.read().map_err(|e| {
                        McpError::internal_error(format!("Lock error: {}", e), None)
                    })?;
                    replay_items(&idx, &items)
                };

                {
                    let mut ctx = self.context.lock().unwrap();
                    ctx.active = Some(name.clone());
                    ctx.items = items;
                }
                save_context(&name, &self.context.lock().unwrap().items);

                let msg = format!(
                    "Context \"{}\" activated ({} items)\n\n{}",
                    name, item_count, replay_text
                );
                Ok(CallToolResult::success(vec![Content::text(msg)]))
            }
            None => {
                //  Create new context
                {
                    let mut ctx = self.context.lock().unwrap();
                    ctx.active = Some(name.clone());
                    ctx.items.clear();
                }
                save_context(&name, &[]);

                Ok(CallToolResult::success(vec![Content::text(format!(
                    "Context \"{}\" created and activated.", name
                ))]))
            }
        }
    }

    #[tool(description = "Search Verus proof/spec/exec functions by name substring. Returns matching function signatures with module paths and file locations.")]
    pub async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let kind = params.kind.as_deref().and_then(parse_kind);
        let default_limit = if params.details { 10 } else { MAX_RESULTS };
        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(default_limit);
        let offset = params.offset.unwrap_or(0);
        let result = idx.search(
            &params.query,
            kind,
            params.crate_name.as_deref(),
            params.module.as_deref(),
            params.trait_only,
            offset,
            limit,
        );

        //  Auto-capture to context when 1-2 results
        if result.total_count >= 1 && result.total_count <= 2 {
            self.capture_names(result.items.iter().map(|e| &e.name));
        }

        let mut text: String = result
            .items
            .iter()
            .map(|e| if params.details { e.format_full() } else { e.format_compact() })
            .collect::<Vec<_>>()
            .join("\n");

        //  When substring results are few and no offset, append fuzzy matches
        if offset == 0 && result.total_count < 5 {
            let fuzzy_limit = if result.items.is_empty() { 10 } else { DEFAULT_RESULTS.saturating_sub(result.items.len()) };
            if fuzzy_limit > 0 {
                let fuzzy = idx.search_fuzzy(&params.query, fuzzy_limit);
                //  Filter out items already in substring results
                let existing: std::collections::HashSet<(&str, usize)> = result
                    .items
                    .iter()
                    .map(|e| (e.file_path.as_str(), e.line))
                    .collect();
                let fuzzy_new: Vec<_> = fuzzy
                    .items
                    .iter()
                    .filter(|e| !existing.contains(&(e.file_path.as_str(), e.line)))
                    .collect();
                if !fuzzy_new.is_empty() {
                    text.push_str("\n\n--- fuzzy matches ---\n");
                    for e in &fuzzy_new {
                        text.push_str(&format!("{}\n", e.format_compact()));
                    }
                }
            }
        }

        if result.items.is_empty() && text.trim().is_empty() {
            let mut msg = format!("No results for '{}'", params.query);

            //  Note active filters and check if removing them helps
            let has_filters = params.kind.is_some()
                || params.crate_name.is_some()
                || params.module.is_some()
                || params.trait_only;
            if has_filters {
                let mut filter_parts = Vec::new();
                if let Some(ref k) = params.kind {
                    filter_parts.push(format!("kind={}", k));
                }
                if let Some(ref c) = params.crate_name {
                    filter_parts.push(format!("crate={}", c));
                }
                if let Some(ref m) = params.module {
                    filter_parts.push(format!("module={}", m));
                }
                if params.trait_only {
                    filter_parts.push("trait_only".to_string());
                }
                let unfiltered = idx.search(&params.query, None, None, None, false, 0, 1);
                if unfiltered.total_count > 0 {
                    msg = format!(
                        "No results for '{}' with {} ({} matches without filters)",
                        params.query,
                        filter_parts.join(", "),
                        unfiltered.total_count
                    );
                }
            }

            msg.push_str(&format_did_you_mean(&idx, &params.query));

            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }

        let count = format_count(result.items.len(), result.total_count, offset);
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}:\n\n{}",
            count, text
        ))]))
    }

    #[tool(description = "Look up a Verus function or type by exact name. Returns full signature with requires/ensures clauses for functions, or field/variant details for types.")]
    pub async fn lookup(
        &self,
        Parameters(params): Parameters<LookupParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let fn_results = idx.lookup(&params.name);

        if !fn_results.is_empty() {
            let text: String = fn_results
                .iter()
                .map(|e| e.format_full())
                .collect::<Vec<_>>()
                .join("\n");
            self.capture_names(std::iter::once(&params.name));
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        //  Fallback: search types
        let type_results = idx.lookup_type(&params.name);

        if !type_results.is_empty() {
            let text: String = type_results
                .iter()
                .map(|e| e.format_full())
                .collect::<Vec<_>>()
                .join("\n");
            self.capture_names(std::iter::once(&params.name));
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let mut msg = format!("No function or type named '{}'", params.name);
        msg.push_str(&format_did_you_mean(&idx, &params.name));
        Ok(CallToolResult::success(vec![Content::text(msg)]))
    }

    #[tool(description = "Look up a Verus function by exact name and return its full source code (signature + body). Reads the actual source file using the indexed line range.")]
    pub async fn lookup_source(
        &self,
        Parameters(params): Parameters<LookupParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let fn_results = idx.lookup(&params.name);

        if fn_results.is_empty() {
            let mut msg = format!("No function named '{}'", params.name);
            msg.push_str(&format_did_you_mean(&idx, &params.name));
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.capture_names(std::iter::once(&params.name));

        let mut sections = Vec::new();
        for e in &fn_results {
            //  Read source lines from disk
            match std::fs::read_to_string(&e.file_path) {
                Ok(contents) => {
                    let lines: Vec<&str> = contents.lines().collect();
                    let start = e.line.saturating_sub(1); //  1-indexed to 0-indexed
                    let end = e.end_line.min(lines.len());
                    let source: String = lines[start..end]
                        .join("\n");
                    sections.push(format!(
                        "//  {}:{}-{}\n{}",
                        e.file_path, e.line, e.end_line, source
                    ));
                }
                Err(err) => {
                    sections.push(format!(
                        "//  {}:{}-{} (could not read: {})",
                        e.file_path, e.line, e.end_line, err
                    ));
                }
            }
        }

        Ok(CallToolResult::success(vec![Content::text(
            sections.join("\n---\n"),
        )]))
    }

    #[tool(description = "Look up multiple Verus functions/types by exact name in one call. Returns full signatures with requires/ensures clauses. Max 10 names per call.")]
    pub async fn batch_lookup(
        &self,
        Parameters(params): Parameters<BatchLookupParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        if params.names.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No names provided",
            )]));
        }
        if params.names.len() > 10 {
            return Ok(CallToolResult::success(vec![Content::text(
                "Max 10 names per batch_lookup call",
            )]));
        }

        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let mut sections = Vec::new();
        let mut found: Vec<&str> = Vec::new();
        for name in &params.names {
            let fn_results = idx.lookup(name);
            if !fn_results.is_empty() {
                found.push(name.as_str());
                let text: String = fn_results
                    .iter()
                    .map(|e| e.format_full())
                    .collect::<Vec<_>>()
                    .join("\n");
                sections.push(text);
                continue;
            }
            let type_results = idx.lookup_type(name);
            if !type_results.is_empty() {
                found.push(name.as_str());
                let text: String = type_results
                    .iter()
                    .map(|e| e.format_full())
                    .collect::<Vec<_>>()
                    .join("\n");
                sections.push(text);
                continue;
            }
            sections.push(format!("'{}': not found", name));
        }
        self.capture_names(&found);

        Ok(CallToolResult::success(vec![Content::text(
            sections.join("\n---\n"),
        )]))
    }

    #[tool(description = "Search within ensures clauses of Verus functions. Useful for finding lemmas that prove a specific property. Query supports regex (e.g., 'div.*mul.*eqv'); falls back to substring if not valid regex.")]
    pub async fn search_ensures(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let kind = params.kind.as_deref().and_then(parse_kind);
        let result = idx.search_ensures(&params.query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit);

        //  Auto-capture to context when 1-2 results
        if result.total_count >= 1 && result.total_count <= 2 {
            self.capture_names(result.items.iter().map(|e| &e.name));
        }

        if result.items.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No ensures clauses matching '{}'",
                params.query
            ))]));
        }

        let matcher = Matcher::new(&params.query);
        let text: String = result
            .items
            .iter()
            .map(|e| e.format_clause_match(&e.ensures, &|s| matcher.find_pos(s)))
            .collect::<Vec<_>>()
            .join("\n");

        let count = format_count(result.items.len(), result.total_count, offset);
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}:\n\n{}",
            count, text
        ))]))
    }

    #[tool(description = "Search within requires clauses of Verus functions. Useful for finding what preconditions lemmas need. Query supports regex (e.g., 'div.*mul'); falls back to substring if not valid regex.")]
    pub async fn search_requires(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let kind = params.kind.as_deref().and_then(parse_kind);
        let result = idx.search_requires(&params.query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit);

        if result.items.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No requires clauses matching '{}'",
                params.query
            ))]));
        }

        let matcher = Matcher::new(&params.query);
        let text: String = result
            .items
            .iter()
            .map(|e| e.format_clause_match(&e.requires, &|s| matcher.find_pos(s)))
            .collect::<Vec<_>>()
            .join("\n");

        let count = format_count(result.items.len(), result.total_count, offset);
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}:\n\n{}",
            count, text
        ))]))
    }

    #[tool(description = "Search function bodies for usage of a lemma or pattern. Useful for finding where a specific lemma is called. Query supports regex (e.g., 'lemma_.*cancel'); falls back to substring if not valid regex.")]
    pub async fn search_body(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let kind = params.kind.as_deref().and_then(parse_kind);
        let result = idx.search_body(&params.query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit);

        if result.items.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No function bodies matching '{}'",
                params.query
            ))]));
        }

        let matcher = Matcher::new(&params.query);
        let text: String = result
            .items
            .iter()
            .map(|e| e.format_body_match(&|s| matcher.find_pos(s)))
            .collect::<Vec<_>>()
            .join("\n");

        let count = format_count(result.items.len(), result.total_count, offset);
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}:\n\n{}",
            count, text
        ))]))
    }

    #[tool(description = "Search within doc comments of Verus functions and types. Query supports regex; falls back to substring if not valid regex.")]
    pub async fn search_doc(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);

        //  Search both functions and types
        let kind = params.kind.as_deref().and_then(parse_kind);
        let fn_result = idx.search_doc(&params.query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit);
        let type_result = idx.search_type_doc(&params.query, params.crate_name.as_deref(), params.module.as_deref(), offset, limit);

        if fn_result.items.is_empty() && type_result.items.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No doc comments matching '{}'",
                params.query
            ))]));
        }

        let mut parts = Vec::new();
        if !fn_result.items.is_empty() {
            let text: String = fn_result
                .items
                .iter()
                .map(|e| {
                    let doc = e.doc_comment.as_deref().unwrap_or("");
                    format!("[{}] {}  ({}:{})\n    {}", e.kind, e.name, e.file_path.rsplit('/').next().unwrap_or(&e.file_path), e.line, doc)
                })
                .collect::<Vec<_>>()
                .join("\n");
            let count = format_count(fn_result.items.len(), fn_result.total_count, offset);
            parts.push(format!("{} (functions):\n\n{}", count, text));
        }
        if !type_result.items.is_empty() {
            let text: String = type_result
                .items
                .iter()
                .map(|e| {
                    let doc = e.doc_comment.as_deref().unwrap_or("");
                    format!("[{}] {}  ({}:{})\n    {}", e.item_kind, e.name, e.file_path.rsplit('/').next().unwrap_or(&e.file_path), e.line, doc)
                })
                .collect::<Vec<_>>()
                .join("\n");
            let count = format_count(type_result.items.len(), type_result.total_count, offset);
            parts.push(format!("{} (types):\n\n{}", count, text));
        }

        Ok(CallToolResult::success(vec![Content::text(
            parts.join("\n\n"),
        )]))
    }

    #[tool(description = "Search Verus types (structs, enums, type aliases) by name substring.")]
    pub async fn search_types(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let result = idx.search_types(&params.query, params.crate_name.as_deref(), params.module.as_deref(), offset, limit);

        if result.items.is_empty() {
            let mut msg = format!("No types matching '{}'", params.query);
            msg.push_str(&format_did_you_mean(&idx, &params.query));
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }

        let text: String = result
            .items
            .iter()
            .map(|e| e.format_compact())
            .collect::<Vec<_>>()
            .join("\n");

        let count = format_count(result.items.len(), result.total_count, offset);
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}:\n\n{}",
            count, text
        ))]))
    }

    #[tool(description = "List all functions and types in a module. Use exact or prefix module path (e.g., 'crate::orient2d' or 'crate').")]
    pub async fn browse_module(
        &self,
        Parameters(params): Parameters<LookupParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let (fns, types) = idx.browse_module(&params.name);

        if fns.is_empty() && types.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No items in module '{}'",
                params.name
            ))]));
        }

        let mut text = String::new();
        if !types.is_empty() {
            text.push_str(&format!("Types ({}):\n", types.len()));
            for t in &types {
                text.push_str(&format!("  {}\n", t.format_compact()));
            }
            text.push('\n');
        }
        if !fns.is_empty() {
            text.push_str(&format!("Functions ({}):\n", fns.len()));
            for f in &fns {
                text.push_str(&format!("  {}\n", f.format_compact()));
            }
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Search for a trait definition and all its implementors. Shows trait methods, supertraits, and all impl blocks.")]
    pub async fn search_trait(
        &self,
        Parameters(params): Parameters<LookupParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let traits = idx.lookup_trait(&params.name);
        let impls = idx.search_trait_impls(&params.name);

        if traits.is_empty() && impls.is_empty() {
            let mut msg = format!("No trait or impls matching '{}'", params.name);
            msg.push_str(&format_did_you_mean(&idx, &params.name));
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }

        let mut text = String::new();
        for t in &traits {
            text.push_str(&t.format_full());
            text.push('\n');
        }
        if !impls.is_empty() {
            text.push_str(&format!("Implementations ({}):\n", impls.len()));
            for i in &impls {
                text.push_str(&format!("  {}\n", i.format_compact()));
            }
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "List all indexed modules with their item counts.")]
    pub async fn list_modules(&self) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let modules = idx.list_modules();
        let total = idx.len() + idx.type_len();

        //  Group modules by crate
        let mut crates: std::collections::BTreeMap<String, Vec<(String, usize)>> =
            std::collections::BTreeMap::new();
        for (path, count) in &modules {
            //  module_path is like "verus_algebra::ring_lemmas" — crate is first segment
            let crate_name = path.split("::").next().unwrap_or(path);
            let mod_name = path.splitn(2, "::").nth(1).unwrap_or("(root)");
            crates
                .entry(crate_name.to_string())
                .or_default()
                .push((mod_name.to_string(), *count));
        }

        let mut text = format!("{} items, {} modules\n\n", total, modules.len());
        for (crate_name, mods) in &crates {
            let crate_total: usize = mods.iter().map(|(_, c)| c).sum();
            let mod_list: Vec<String> = mods.iter().map(|(m, c)| format!("{}({})", m, c)).collect();
            text.push_str(&format!(
                "{} ({}): {}\n",
                crate_name,
                crate_total,
                mod_list.join(", ")
            ));
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Show index statistics: function counts by kind (spec/proof/exec), by crate, type/trait counts, and assume(false) proof debt.")]
    pub async fn stats(&self) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let s = idx.stats();

        let mut text = format!(
            "Total: {} functions, {} types, {} traits\n\
             By kind: {} spec, {} proof, {} exec\n\
             Proof debt: {} assume(false)\n",
            s.total_functions, s.total_types, s.total_traits,
            s.spec, s.proof, s.exec,
            s.assume_false,
        );

        text.push_str("\nBy crate:\n");
        for (name, cs) in &s.by_crate {
            let mut parts = vec![format!("{} fns", cs.functions)];
            if cs.types > 0 {
                parts.push(format!("{} types", cs.types));
            }
            if cs.traits > 0 {
                parts.push(format!("{} traits", cs.traits));
            }
            if cs.assume_false > 0 {
                parts.push(format!("{} assume(false)", cs.assume_false));
            }
            text.push_str(&format!("  {}: {}\n", name, parts.join(", ")));
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Search Verus functions by parameter types, return type, or trait bounds. At least one of param_type, return_type, or type_bound is required. Examples: param_type='Vec2' finds orient2d/det2d; return_type='bool' finds predicates; type_bound='OrderedField' finds intersection functions; combine param_type='Point3' + name='orient' for orient3d family.")]
    pub async fn search_signature(
        &self,
        Parameters(params): Parameters<SignatureSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        if params.param_type.is_none() && params.return_type.is_none() && params.type_bound.is_none() {
            return Ok(CallToolResult::success(vec![Content::text(
                "At least one of param_type, return_type, or type_bound must be provided.",
            )]));
        }

        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let kind = params.kind.as_deref().and_then(parse_kind);
        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let result = idx.search_signature(
            params.param_type.as_deref(),
            params.return_type.as_deref(),
            params.type_bound.as_deref(),
            params.name.as_deref(),
            kind,
            params.crate_name.as_deref(),
            params.module.as_deref(),
            offset,
            limit,
        );

        if result.items.is_empty() {
            let mut query_desc = Vec::new();
            if let Some(ref p) = params.param_type { query_desc.push(format!("param_type={}", p)); }
            if let Some(ref r) = params.return_type { query_desc.push(format!("return_type={}", r)); }
            if let Some(ref t) = params.type_bound { query_desc.push(format!("type_bound={}", t)); }
            if let Some(ref n) = params.name { query_desc.push(format!("name={}", n)); }
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No results for signature search: {}",
                query_desc.join(", ")
            ))]));
        }

        let text: String = result
            .items
            .iter()
            .map(|e| e.format_compact())
            .collect::<Vec<_>>()
            .join("\n");

        let count = format_count(result.items.len(), result.total_count, offset);
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}:\n\n{}",
            count, text
        ))]))
    }

    #[tool(description = "Find callers or callees of a function (call graph). Direction: 'callers' (default) shows who calls this function; 'callees' shows what this function calls.")]
    pub async fn find_dependencies(
        &self,
        Parameters(params): Parameters<DependencyParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_not_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let direction = params.direction.as_deref().unwrap_or("callers");

        match direction {
            "callees" => {
                let callees = idx.find_callees(&params.name);
                if callees.is_empty() {
                    let mut msg = format!(
                        "'{}' calls no known functions (or has no body)",
                        params.name
                    );
                    msg.push_str(&format_did_you_mean(&idx, &params.name));
                    return Ok(CallToolResult::success(vec![Content::text(msg)]));
                }
                let mut sorted = callees;
                sorted.sort();
                Ok(CallToolResult::success(vec![Content::text(format!(
                    "'{}' calls {} functions:\n\n{}",
                    params.name,
                    sorted.len(),
                    sorted.join("\n")
                ))]))
            }
            _ => {
                //  "callers" (default)
                let callers = idx.find_callers(&params.name);
                if callers.is_empty() {
                    let mut msg = format!("No callers found for '{}'", params.name);
                    msg.push_str(&format_did_you_mean(&idx, &params.name));
                    return Ok(CallToolResult::success(vec![Content::text(msg)]));
                }
                let text: String = callers
                    .iter()
                    .map(|e| e.format_compact())
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(CallToolResult::success(vec![Content::text(format!(
                    "{} callers of '{}':\n\n{}",
                    callers.len(),
                    params.name,
                    text
                ))]))
            }
        }
    }

    #[tool(description = "Run Verus verification.

crate_name → crate directory to verify (e.g. 'verus-geometry').
module (optional) → verify only one module for faster iteration. Accepts file path ('src/runtime/polygon.rs') or module path ('runtime::polygon').
raw (optional) → when true, return raw compiler output instead of parsed diagnostics.

On success: clean summary. On failure: extracted error diagnostics with function context (which function, relative line within it). Timeout: 10 minutes.")]
    pub async fn check(
        &self,
        Parameters(params): Parameters<CheckParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let workspace = indexer::find_workspace_root();
        let crate_dir = workspace.join(&params.crate_name);

        if !crate_dir.join("src").is_dir() {
            let mut available = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&workspace) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("verus-") && entry.path().join("src").is_dir() {
                        available.push(name);
                    }
                }
            }
            available.sort();
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Crate '{}' not found\n\nAvailable crates: {}",
                params.crate_name,
                available.join(", ")
            ))]));
        }

        let default_verus_root = workspace.join("verus-dev");
        let module_flag = match params.module {
            Some(ref m) => match validate_module(&params.crate_name, m, &crate_dir) {
                Ok(flag) => flag,
                Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
            },
            None => String::new(),
        };
        let is_raw = params.raw.unwrap_or(false);
        //  raw mode: no JSON, human-readable output
        //  normal mode: JSON for structured diagnostics
        let script = build_check_script(
            &default_verus_root, &params.crate_name, &module_flag, !is_raw);
        let output = match run_bash_script(&script, &crate_dir).await {
            Ok(output) => output,
            Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
        };
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        if is_raw {
            return Ok(CallToolResult::success(vec![Content::text(
                format!("{}{}", stdout, stderr),
            )]));
        }

        //  Check if dependencies were compiled during this run.
        //  If so, the verification counts may reflect dependency results, not the target.
        if has_dependency_compilation(&stderr, &params.crate_name) {
            //  Check if the build itself failed (dependency or target)
            let has_build_error = stderr.contains("error[E")
                || stderr.contains("could not compile")
                || stdout.contains("error[E");
            if has_build_error {
                //  Build failed — report the error from this run rather than rerunning.
                //  Parse diagnostics to show what went wrong.
                let diagnostics = Self::parse_json_diagnostics(&stdout, true);
                if !diagnostics.is_empty() {
                    let annotated = self.annotate_diagnostics(&diagnostics, &params.crate_name);
                    return Ok(CallToolResult::success(vec![Content::text(format!(
                        "Build failed (dependency compilation detected)\n\n{}",
                        annotated.join("\n\n")
                    ))]));
                }
                let rendered = Self::extract_rendered_text(&stdout);
                let fallback = if !rendered.is_empty() { rendered } else { stderr.clone() };
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "Build failed (dependency compilation detected)\n\n{}", fallback
                ))]));
            }

            //  Dependencies compiled successfully — rerun so deps are cached
            //  and we get counts scoped to the target crate only.
            let output = match run_bash_script(&script, &crate_dir).await {
                Ok(output) => output,
                Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
            };
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return self.parse_verus_output(&params.crate_name, &stdout, &stderr, None);
        }

        self.parse_verus_output(&params.crate_name, &stdout, &stderr, None)
    }

    ///  Parse cargo verus output into a structured result.
    ///  stdout contains JSON lines from `--message-format=json` (plus possibly the
    ///  verification summary from the verus compiler's println!).
    ///  stderr contains cargo progress messages and fancy notes.
    fn parse_verus_output(
        &self,
        crate_name: &str,
        stdout: &str,
        stderr: &str,
        note: Option<&str>,
    ) -> Result<CallToolResult, McpError> {
        let note_prefix = note.map(|n| format!("{}\n\n", n)).unwrap_or_default();

        //  See `pick_verus_summary` for the rationale on multi-summary handling.
        let combined = format!("{}\n{}", stdout, stderr);
        let summary = pick_verus_summary(&combined);

        //  Determine if there are errors to filter warnings
        let has_errors = summary.map(|(_, e, _)| e > 0).unwrap_or(false);

        //  Parse JSON diagnostics from stdout (suppress warnings when errors exist)
        let diagnostics = Self::parse_json_diagnostics(stdout, has_errors);

        if let Some((verified, errors, cached)) = summary {
            let cached_msg = if cached > 0 { format!(", {} cached", cached) } else { String::new() };

            if errors == 0 {
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "{}{}: {} verified, 0 errors{}",
                    note_prefix, crate_name, verified, cached_msg
                ))]));
            }

            //  We have errors — use JSON diagnostics
            if !diagnostics.is_empty() {
                let annotated = self.annotate_diagnostics(&diagnostics, crate_name);
                let mut text = format!("{}{}", note_prefix, annotated.join("\n\n"));
                text.push_str(&format!(
                    "\n\n{}: {} verified, {} errors{}",
                    crate_name, verified, errors, cached_msg
                ));
                return Ok(CallToolResult::success(vec![Content::text(text)]));
            }

            //  JSON parse failed — extract rendered text from JSON lines, fall back to stderr
            let rendered = Self::extract_rendered_text(stdout);
            let fallback = if !rendered.is_empty() { rendered } else { stderr.to_string() };
            let mut text = format!("{}{}", note_prefix, fallback);
            text.push_str(&format!(
                "\n\n{}: {} verified, {} errors{}",
                crate_name, verified, errors, cached_msg
            ));
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        //  No verification summary — likely a build error.
        if !diagnostics.is_empty() {
            let annotated = self.annotate_diagnostics(&diagnostics, crate_name);
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "{}No verification summary found (build error?)\n\n{}",
                note_prefix,
                annotated.join("\n\n")
            ))]));
        }

        //  Fallback: last 50 lines of stderr
        let lines: Vec<&str> = stderr.lines().collect();
        let start = lines.len().saturating_sub(50);
        let tail = lines[start..].join("\n");
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}No verification summary found (build error?)\n\n{}",
            note_prefix, tail
        ))]))
    }

    ///  Extract human-readable `rendered` text from JSON stdout lines.
    ///  Used as fallback when full JSON diagnostic parsing fails.
    fn extract_rendered_text(stdout: &str) -> String {
        let mut rendered_parts = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for line in stdout.lines() {
            let line = line.trim();
            if !line.starts_with('{') {
                continue;
            }
            let Ok(cargo_line) = serde_json::from_str::<CargoJsonLine>(line) else {
                continue;
            };
            if cargo_line.reason != "compiler-message" {
                continue;
            }
            if let Some(msg) = cargo_line.message {
                if let Some(rendered) = msg.rendered {
                    let trimmed = rendered.trim().to_string();
                    if !trimmed.is_empty() && seen.insert(trimmed.clone()) {
                        rendered_parts.push(trimmed);
                    }
                }
            }
        }
        rendered_parts.join("\n\n")
    }

    ///  Parse JSON diagnostic messages from `--message-format=json` stdout.
    ///  Returns only error/warning/note diagnostics (filters out artifacts, build-script, etc.).
    ///  When `errors_only` is true, suppresses warnings and notes to reduce noise.
    fn parse_json_diagnostics(stdout: &str, errors_only: bool) -> Vec<DiagMessage> {
        let mut diagnostics = Vec::new();
        let mut seen_rendered = std::collections::HashSet::new();

        for line in stdout.lines() {
            let line = line.trim();
            if !line.starts_with('{') {
                continue;
            }
            let Ok(cargo_line) = serde_json::from_str::<CargoJsonLine>(line) else {
                continue;
            };
            if cargo_line.reason != "compiler-message" {
                continue;
            }
            let Some(msg) = cargo_line.message else {
                continue;
            };
            //  Skip noise
            if msg.level == "failure-note" {
                continue;
            }
            if msg.message.starts_with("aborting due to") {
                continue;
            }
            //  When we know there are errors, skip warnings/notes to reduce noise
            if errors_only && msg.level != "error" {
                continue;
            }
            //  Deduplicate by rendered text
            if let Some(ref rendered) = msg.rendered {
                if !seen_rendered.insert(rendered.clone()) {
                    continue;
                }
            }
            diagnostics.push(msg);
        }
        diagnostics
    }

    ///  Annotate diagnostics with function context: show function source with
    ///  error messages inlined. Groups multiple errors in the same function.
    fn annotate_diagnostics(
        &self,
        diagnostics: &[DiagMessage],
        crate_name: &str,
    ) -> Vec<String> {
        let idx = self.index.read().ok();
        let workspace = indexer::find_workspace_root();

        //  Cache file contents to avoid re-reading
        let mut file_cache: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        //  Group errors by function key (file_path, fn_line, fn_end_line).
        //  Preserve insertion order with Vec of unique keys.
        let mut fn_keys: Vec<(String, usize, usize)> = Vec::new();
        //  Map: fn_key → Vec<(err_line_0indexed, err_msg)>
        let mut fn_errors: std::collections::HashMap<(String, usize, usize), Vec<(usize, String)>> =
            std::collections::HashMap::new();
        //  Diagnostics that couldn't be associated with a function
        let mut orphan_rendered: Vec<String> = Vec::new();

        for diag in diagnostics {
            //  Find the primary span
            let primary_span = diag.spans.iter().find(|s| s.is_primary);
            let Some(span) = primary_span else {
                //  No primary span — use rendered text
                if let Some(ref rendered) = diag.rendered {
                    orphan_rendered.push(rendered.trim().to_string());
                } else {
                    orphan_rendered.push(format!("{}: {}", diag.level, diag.message));
                }
                continue;
            };

            let qualified_file = format!("{}/{}", crate_name, span.file_name);
            let primary_line = span.line_start;

            let entry = idx.as_deref().and_then(|i| i.fn_at_line(&qualified_file, primary_line));
            let Some(entry) = entry else {
                if let Some(ref rendered) = diag.rendered {
                    orphan_rendered.push(rendered.trim().to_string());
                } else {
                    orphan_rendered.push(format!("{}: {}", diag.level, diag.message));
                }
                continue;
            };

            //  Collect cross-file secondary spans (e.g. trait postcondition errors
            //  where the primary span is the trait definition but each impl is in
            //  a different file). Include them in the error message so the errors
            //  are distinguishable.
            let mut cross_file_labels: Vec<String> = Vec::new();
            for sec_span in diag.spans.iter().filter(|s| !s.is_primary) {
                let sec_file = format!("{}/{}", crate_name, sec_span.file_name);
                if sec_file != qualified_file {
                    cross_file_labels.push(format!(
                        "{}:{}",
                        sec_span.file_name, sec_span.line_start,
                    ));
                }
            }

            let err_msg = if cross_file_labels.is_empty() {
                format!("{}: {}", diag.level, diag.message)
            } else {
                format!("{}: {} ({})", diag.level, diag.message, cross_file_labels.join("; "))
            };
            let err_line_0idx = primary_line.saturating_sub(1); //  0-indexed
            let key = (entry.file_path.clone(), entry.line, entry.end_line);
            if !fn_errors.contains_key(&key) {
                fn_keys.push(key.clone());
            }
            fn_errors
                .entry(key.clone())
                .or_default()
                .push((err_line_0idx, err_msg));

            //  Also include secondary spans as additional context
            for sec_span in diag.spans.iter().filter(|s| !s.is_primary) {
                if let Some(ref label) = sec_span.label {
                    let sec_file = format!("{}/{}", crate_name, sec_span.file_name);
                    //  Only include if it's in the same function
                    if sec_file == qualified_file
                        && sec_span.line_start >= entry.line
                        && sec_span.line_start <= entry.end_line
                    {
                        fn_errors
                            .entry(key.clone())
                            .or_default()
                            .push((sec_span.line_start.saturating_sub(1), format!("  → {}", label)));
                    }
                }
            }
        }

        let mut results: Vec<String> = Vec::new();

        for key in &fn_keys {
            let (ref file_path, fn_line, fn_end_line) = *key;
            let errs = &fn_errors[key];

            let lines = file_cache
                .entry(file_path.clone())
                .or_insert_with(|| {
                    //  file_path is a display path like "verus-bigint/src/foo.rs";
                    //  resolve to absolute using workspace root
                    let abs_path = workspace.join(file_path);
                    std::fs::read_to_string(&abs_path)
                        .unwrap_or_default()
                        .lines()
                        .map(|l| l.to_string())
                        .collect()
                });

            if lines.is_empty() {
                for (_, msg) in errs {
                    results.push(msg.clone());
                }
                continue;
            }

            let fn_start = fn_line.saturating_sub(1); //  0-indexed
            let fn_end = fn_end_line.min(lines.len());
            let fn_len = fn_end.saturating_sub(fn_start);

            //  Collect error lines into a map: line_idx → Vec<msg>
            let mut err_map: std::collections::BTreeMap<usize, Vec<&str>> =
                std::collections::BTreeMap::new();
            for (err_idx, msg) in errs {
                err_map.entry(*err_idx).or_default().push(msg.as_str());
            }

            let mut out = Vec::new();

            if fn_len <= 100 {
                //  Short function: show entire source with errors inlined
                for i in fn_start..fn_end {
                    out.push(lines[i].clone());
                    if let Some(msgs) = err_map.get(&i) {
                        let indent = lines[i].len() - lines[i].trim_start().len();
                        for msg in msgs {
                            out.push(format!(
                                "{}//  ^^^ {}",
                                " ".repeat(indent),
                                msg
                            ));
                        }
                    }
                }
            } else {
                //  Long function: show signature + context windows around each error

                //  Find where the body starts (first `{` line)
                let mut body_start = fn_start;
                for i in fn_start..fn_end {
                    if lines[i].contains('{') {
                        body_start = i;
                        break;
                    }
                }

                //  Build merged context windows around all error lines
                let ctx = 3usize;
                let mut windows: Vec<(usize, usize)> = Vec::new();
                for &err_idx in err_map.keys() {
                    let w_start = err_idx.saturating_sub(ctx).max(body_start + 1);
                    let w_end = (err_idx + ctx + 1).min(fn_end);
                    if let Some(last) = windows.last_mut() {
                        if w_start <= last.1 {
                            last.1 = last.1.max(w_end);
                            continue;
                        }
                    }
                    windows.push((w_start, w_end));
                }

                //  Signature up to `{` line (also check for errors on signature lines)
                for i in fn_start..=body_start.min(fn_end.saturating_sub(1)) {
                    out.push(lines[i].clone());
                    if let Some(msgs) = err_map.get(&i) {
                        let indent = lines[i].len() - lines[i].trim_start().len();
                        for msg in msgs {
                            out.push(format!(
                                "{}//  ^^^ {}",
                                " ".repeat(indent),
                                msg
                            ));
                        }
                    }
                }

                for (w_idx, &(w_start, w_end)) in windows.iter().enumerate() {
                    let prev_end = if w_idx == 0 { body_start + 1 } else { windows[w_idx - 1].1 };
                    if w_start > prev_end {
                        out.push("    ...".to_string());
                    }

                    for i in w_start..w_end {
                        out.push(lines[i].clone());
                        if let Some(msgs) = err_map.get(&i) {
                            let indent = lines[i].len() - lines[i].trim_start().len();
                            for msg in msgs {
                                out.push(format!(
                                    "{}//  ^^^ {}",
                                    " ".repeat(indent),
                                    msg
                                ));
                            }
                        }
                    }
                }

                let last_window_end = windows.last().map(|w| w.1).unwrap_or(body_start + 1);
                if last_window_end < fn_end.saturating_sub(1) {
                    out.push("    ...".to_string());
                }

                if fn_end > 0 && fn_end - 1 >= last_window_end {
                    out.push(lines[fn_end - 1].clone());
                }
            }

            results.push(out.join("\n"));
        }

        //  Append orphan diagnostics (not associated with any function)
        results.extend(orphan_rendered);
        results
    }

    #[tool(description = "Profile Verus verification performance.

Returns per-function SMT time and rlimit breakdown sorted by cost. Use rlimit (deterministic) not SMT time (high variance) to measure optimization impact.

crate_name → crate directory to profile.
module (optional) → profile only one module.
top_n (optional) → number of top functions to show (default 25).

Timeout: 10 minutes.")]
    pub async fn profile(
        &self,
        Parameters(params): Parameters<ProfileParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let workspace = indexer::find_workspace_root();
        let crate_dir = workspace.join(&params.crate_name);

        if !crate_dir.join("src").is_dir() {
            let mut available = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&workspace) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("verus-") && entry.path().join("src").is_dir() {
                        available.push(name);
                    }
                }
            }
            available.sort();
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Crate '{}' not found\n\nAvailable crates: {}",
                params.crate_name,
                available.join(", ")
            ))]));
        }

        let default_verus_root = workspace.join("verus-dev");
        let module_flag = match params.module {
            Some(ref m) => match validate_module(&params.crate_name, m, &crate_dir) {
                Ok(flag) => flag,
                Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
            }
            None => String::new(),
        };
        let top_n = params.top_n.unwrap_or(25);

        let bash_preamble = build_profile_preamble(
            &default_verus_root,
            &params.crate_name,
            &module_flag,
            top_n,
        );

        let python_part = r#"import json, sys

json_path = sys.argv[1]
top_n = int(sys.argv[2])

with open(json_path) as f:
    raw = f.read().strip()

json_start = raw.find('{')
if json_start < 0:
    print(f"error: no JSON object found in output.\n{raw[:500]}", file=sys.stderr)
    sys.exit(1)

import re
json_text = raw[json_start:]
json_text = re.sub(r',\s*([}\]])', r'\1', json_text)
data, _ = json.JSONDecoder().raw_decode(json_text)

times = data.get("times-ms", {})
smt = times.get("smt", {})
verified = data.get("verification-results", {}).get("verified", "?")
errors = data.get("verification-results", {}).get("errors", "?")

funcs = []
for mod in smt.get("smt-run-module-times", []):
    for fn in mod.get("function-breakdown", []):
        name = fn["function"].split("::")[-1]
        module = mod["module"]
        funcs.append({
            "name": name,
            "module": module,
            "time_us": fn["time-micros"],
            "rlimit": fn["rlimit"],
            "ok": fn.get("success", True),
        })

funcs.sort(key=lambda x: x["rlimit"], reverse=True)
total_us = sum(f["time_us"] for f in funcs)
total_rl = sum(f["rlimit"] for f in funcs)

lines = []
lines.append(f"{verified} verified, {errors} errors")
lines.append("")
lines.append(f"{'#':>3}  {'Function':<48} {'Time':>10} {'Rlimit':>12}  {'Module'}")
lines.append("-" * 100)

for i, fn in enumerate(funcs[:top_n]):
    ms = fn["time_us"] / 1000
    rlimit_s = f"{fn['rlimit']:,}"
    lines.append(f"{i+1:>3}  {fn['name']:<48} {ms:>8.1f}ms {rlimit_s:>12}  {fn['module']}")

lines.append("")

mods = {}
for fn in funcs:
    m = fn["module"]
    if m not in mods:
        mods[m] = {"time_us": 0, "rlimit": 0, "count": 0}
    mods[m]["time_us"] += fn["time_us"]
    mods[m]["rlimit"] += fn["rlimit"]
    mods[m]["count"] += 1

lines.append(f"{'Module':<35} {'Time':>10} {'Rlimit':>14}  {'Fns':>4}")
lines.append("-" * 72)
for m, v in sorted(mods.items(), key=lambda x: x[1]["rlimit"], reverse=True):
    ms = v["time_us"] / 1000
    lines.append(f"{m:<35} {ms:>8.1f}ms {v['rlimit']:>14,}  {v['count']:>4}")

lines.append("")
lines.append(f"Total: {len(funcs)} functions, {total_us/1000:.0f}ms SMT, {total_rl:,} rlimit")

print("\n".join(lines))
PYEOF
"#;

        let script = format!("{}{}", bash_preamble, python_part);
        let output = match run_bash_script(&script, &crate_dir).await {
            Ok(output) => output,
            Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
        };

        //  If --verify-module was used, always do a full crate profile after.
        //  This avoids false positive/negative detection issues.
        if !module_flag.is_empty() {
            let retry_preamble = build_profile_preamble(
                &default_verus_root,
                &params.crate_name,
                "",
                top_n,
            );
            let retry_script = format!("{}{}", retry_preamble, python_part);
            let output = match run_bash_script(&retry_script, &crate_dir).await {
                Ok(output) => output,
                Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
            };
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stdout.trim().is_empty() {
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "(full crate profiled after module check)\n\n{}",
                    stdout
                ))]));
            }
            //  Fallback output also empty - show stderr
            let lines: Vec<&str> = stderr.lines().collect();
            let start = lines.len().saturating_sub(50);
            let tail = lines[start..].join("\n");
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Profile failed\n\n{}", tail
            ))]));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if stdout.trim().is_empty() {
            //  Python or cargo failed — show stderr
            let lines: Vec<&str> = stderr.lines().collect();
            let start = lines.len().saturating_sub(50);
            let tail = lines[start..].join("\n");
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Profile failed\n\n{}", tail
            ))]));
        }

        Ok(CallToolResult::success(vec![Content::text(stdout.to_string())]))
    }

    #[tool(description = "Compile a Verus crate binary using cargo-verus build (no verification).\n\nThis is NOT verification — use `check` to verify proofs. This tool only compiles an executable binary, like `cargo build`.\n\ncrate_name → crate directory to compile (e.g. 'verus-gui').\nfeatures (optional) → cargo features to enable.\nrelease (optional) → compile in release mode.\nextra_args (optional) → extra flags passed to cargo build.\n\nReturns build diagnostics on failure, success message otherwise. Timeout: 10 minutes.")]
    pub async fn compile(
        &self,
        Parameters(params): Parameters<BuildParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let workspace = indexer::find_workspace_root();
        let crate_dir = workspace.join(&params.crate_name);

        if !crate_dir.join("src").is_dir() {
            let mut available = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&workspace) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("verus-") && entry.path().join("src").is_dir() {
                        available.push(name);
                    }
                }
            }
            available.sort();
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Crate '{}' not found\n\nAvailable crates: {}",
                params.crate_name,
                available.join(", ")
            ))]));
        }

        let default_verus_root = workspace.join("verus-dev");
        let release = params.release.unwrap_or(false);
        let cmd_display = format_resolved_build_command(
            &default_verus_root,
            &params.crate_name,
            params.features.as_deref(),
            release,
            params.extra_args.as_deref(),
        );
        let script = build_build_script(
            &default_verus_root,
            &params.crate_name,
            params.features.as_deref(),
            release,
            params.extra_args.as_deref(),
        );
        let output = match run_bash_script(&script, &crate_dir).await {
            Ok(output) => output,
            Err(msg) => return Ok(CallToolResult::success(vec![Content::text(
                format!("$ {}\n\n{}", cmd_display, msg)
            )])),
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        //  Parse JSON diagnostics for errors
        let diagnostics = Self::parse_json_diagnostics(&stdout, true);
        if !diagnostics.is_empty() {
            let annotated = self.annotate_diagnostics(&diagnostics, &params.crate_name);
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "$ {}\n\nBuild failed\n\n{}",
                cmd_display,
                annotated.join("\n\n")
            ))]));
        }

        //  Check for non-JSON build errors in stderr
        let has_error = stderr.contains("error[E") || stderr.contains("could not compile");
        if has_error {
            let lines: Vec<&str> = stderr.lines().collect();
            let start = lines.len().saturating_sub(50);
            let tail = lines[start..].join("\n");
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "$ {}\n\nBuild failed\n\n{}", cmd_display, tail
            ))]));
        }

        let mode = if release { " (release)" } else { "" };
        Ok(CallToolResult::success(vec![Content::text(format!(
            "$ {}\n\n{}: built successfully{}", cmd_display, params.crate_name, mode
        ))]))
    }

    #[tool(description = "Run a Verus crate using cargo-verus run.\n\ncrate_name → crate directory to run (e.g. 'verus-gui').\nfeatures (optional) → cargo features to enable.\nrelease (optional) → run in release mode.\nextra_args (optional) → extra flags passed to cargo run (before --).\nargs (optional) → arguments passed to the binary (after --).\n\nReturns program stdout/stderr output. On build failure: returns diagnostics. Timeout: 10 minutes.")]
    pub async fn run(
        &self,
        Parameters(params): Parameters<RunParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let workspace = indexer::find_workspace_root();
        let crate_dir = workspace.join(&params.crate_name);

        if !crate_dir.join("src").is_dir() {
            let mut available = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&workspace) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("verus-") && entry.path().join("src").is_dir() {
                        available.push(name);
                    }
                }
            }
            available.sort();
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Crate '{}' not found\n\nAvailable crates: {}",
                params.crate_name,
                available.join(", ")
            ))]));
        }

        let default_verus_root = workspace.join("verus-dev");
        let release = params.release.unwrap_or(false);
        let mut cmd_display = format_resolved_build_command(
            &default_verus_root,
            &params.crate_name,
            params.features.as_deref(),
            release,
            params.extra_args.as_deref(),
        );
        if let Some(ref args) = params.args {
            cmd_display.push_str(&format!(" -- {}", args));
        }
        let script = build_run_script(
            &default_verus_root,
            &params.crate_name,
            params.features.as_deref(),
            release,
            params.extra_args.as_deref(),
            params.args.as_deref(),
        );
        let output = match run_bash_script(&script, &crate_dir).await {
            Ok(output) => output,
            Err(msg) => return Ok(CallToolResult::success(vec![Content::text(
                format!("$ {}\n\n{}", cmd_display, msg)
            )])),
        };
        let combined = String::from_utf8_lossy(&output.stdout);

        //  Since run uses 2>&1, all output is in stdout.
        //  Check for build errors first.
        let has_build_error = combined.contains("error[E") || combined.contains("could not compile");
        if has_build_error {
            let lines: Vec<&str> = combined.lines().collect();
            let start = lines.len().saturating_sub(80);
            let tail = lines[start..].join("\n");
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "$ {}\n\nBuild/run failed\n\n{}", cmd_display, tail
            ))]));
        }

        //  Return the program output
        if combined.trim().is_empty() {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "$ {}\n\n{}: ran successfully (no output)", cmd_display, params.crate_name
            ))]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "$ {}\n\n{}", cmd_display, combined
            ))]))
        }
    }

    #[tool(description = "Force rebuild the proof index from disk. Only re-parses files that changed since the last index. Not normally needed — the server auto-reindexes when .rs files change (500ms debounce). Use after external edits or if the index seems stale.")]
    pub async fn reindex(&self) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let old_cache = {
            let idx = self.index.read().map_err(|e| {
                McpError::internal_error(format!("Lock error: {}", e), None)
            })?;
            idx.file_cache().clone()
        };

        let (entries, type_entries, trait_entries, impl_entries, new_cache) =
            indexer::build_index_incremental(&old_cache);
        let fn_count = entries.len();
        let type_count = type_entries.len();
        let new_index = Index::new(entries, type_entries, trait_entries, impl_entries, new_cache);

        let mut idx = self.index.write().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;
        *idx = new_index;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Reindexed: {} fns + {} types",
            fn_count, type_count
        ))]))
    }

    //  -----------------------------------------------------------------------
    //  Code editing tools (standalone mode only)
    //  -----------------------------------------------------------------------

    ///  Gate: require standalone mode for code editing tools.
    fn require_standalone(&self) -> Option<String> {
        if !crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed) {
            Some("Code editing tools are only available in standalone mode.".into())
        } else {
            None
        }
    }

    ///  Gate: block individual search tools in standalone mode (use unified `find` instead).
    fn require_not_standalone(&self) -> Option<String> {
        if crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed) {
            Some("In standalone mode, use the unified `find` tool instead.".into())
        } else {
            None
        }
    }

    ///  Compare use statements before/after a mutation and report only changes.
    fn uses_diff(before: &str, after: &str) -> String {
        let extract = |src: &str| -> std::collections::BTreeSet<String> {
            editor::list_uses(src)
                .unwrap_or_default()
                .lines()
                .filter(|l| !l.is_empty() && *l != "No use statements found.")
                .map(|l| l.to_string())
                .collect()
        };
        let before_uses = extract(before);
        let after_uses = extract(after);
        let mut diff = String::new();
        for u in before_uses.difference(&after_uses) {
            diff.push_str(&format!("- {}\n", u));
        }
        for u in after_uses.difference(&before_uses) {
            diff.push_str(&format!("+ {}\n", u));
        }
        if diff.is_empty() {
            String::new()
        } else {
            format!("\n\nImport changes:\n{}", diff.trim_end())
        }
    }

    #[tool(description = "Search the Verus proof index across all crates.

No scope (default):
  query → fuzzy name search (substring + fuzzy fallback). Set details=true for full signatures.
  name → exact lookup of a function or type (full signature with requires/ensures).
  names → batch exact lookup (max 10).

Scopes:
  ensures — search ensures clauses (regex, e.g. 'div.*mul'). Finds lemmas that prove a property.
  requires — search requires clauses (regex). Finds what preconditions a lemma needs.
  body — search function bodies (regex). Finds where a lemma is called.
  doc — search doc comments (regex). Searches both functions and types.
  types — search structs/enums/type aliases by name substring.
  signature — search by type signature. Set param_type, return_type, and/or type_bound.
  trait — show trait definition + all implementors. Requires name.
  module — list all items in a module. Requires query (module path like 'verus_topology::mesh').
  modules — list all indexed modules grouped by crate.
  dependencies — call graph. Requires name. Set direction='callers' (default) or 'callees'.
  stats — index statistics (counts by kind, by crate, proof debt).
  source — full source code of a function. Requires name.

Filters (work with most scopes): kind, crate_name, module, limit, offset.")]
    pub async fn find(
        &self,
        Parameters(params): Parameters<FindParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let ok = |text: String| -> Result<CallToolResult, McpError> {
            Ok(CallToolResult::success(vec![Content::text(text)]))
        };
        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let kind = params.kind.as_deref().and_then(parse_kind);

        match params.scope.as_deref() {
            Some(scope @ "ensures") | Some(scope @ "requires") | Some(scope @ "body") => {
                let query = params.query.or(params.name.clone())
                    .ok_or_else(|| McpError::invalid_params(format!("query is required for {} search", scope), None))?;
                let result = match scope {
                    "ensures" => idx.search_ensures(&query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit),
                    "requires" => idx.search_requires(&query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit),
                    _ => idx.search_body(&query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit),
                };
                if result.items.is_empty() {
                    return ok(format!("No {} matching '{}'", scope, query));
                }
                let matcher = Matcher::new(&query);
                let text: String = result.items.iter().map(|e| {
                    match scope {
                        "ensures" => e.format_clause_match(&e.ensures, &|s| matcher.find_pos(s)),
                        "requires" => e.format_clause_match(&e.requires, &|s| matcher.find_pos(s)),
                        _ => e.format_body_match(&|s| matcher.find_pos(s)),
                    }
                }).collect::<Vec<_>>().join("\n");
                let count = format_count(result.items.len(), result.total_count, offset);
                ok(format!("{}:\n\n{}", count, text))
            }
            Some("doc") => {
                let query = params.query.or(params.name.clone())
                    .ok_or_else(|| McpError::invalid_params("query is required for doc search", None))?;
                let fn_result = idx.search_doc(&query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit);
                let type_result = idx.search_type_doc(&query, params.crate_name.as_deref(), params.module.as_deref(), offset, limit);
                if fn_result.items.is_empty() && type_result.items.is_empty() {
                    return ok(format!("No doc comments matching '{}'", query));
                }
                let mut parts = Vec::new();
                if !fn_result.items.is_empty() {
                    let text: String = fn_result.items.iter().map(|e| {
                        let doc = e.doc_comment.as_deref().unwrap_or("");
                        format!("[{}] {}  ({}:{})\n    {}", e.kind, e.name, e.file_path.rsplit('/').next().unwrap_or(&e.file_path), e.line, doc)
                    }).collect::<Vec<_>>().join("\n");
                    parts.push(format!("{} (functions):\n\n{}", format_count(fn_result.items.len(), fn_result.total_count, offset), text));
                }
                if !type_result.items.is_empty() {
                    let text: String = type_result.items.iter().map(|e| {
                        let doc = e.doc_comment.as_deref().unwrap_or("");
                        format!("[{}] {}  ({}:{})\n    {}", e.item_kind, e.name, e.file_path.rsplit('/').next().unwrap_or(&e.file_path), e.line, doc)
                    }).collect::<Vec<_>>().join("\n");
                    parts.push(format!("{} (types):\n\n{}", format_count(type_result.items.len(), type_result.total_count, offset), text));
                }
                ok(parts.join("\n\n"))
            }
            Some("types") => {
                let query = params.query.or(params.name)
                    .ok_or_else(|| McpError::invalid_params("query is required for type search", None))?;
                let result = idx.search_types(&query, params.crate_name.as_deref(), params.module.as_deref(), offset, limit);
                if result.items.is_empty() {
                    let mut msg = format!("No types matching '{}'", query);
                    msg.push_str(&format_did_you_mean(&idx, &query));
                    return ok(msg);
                }
                let text: String = result.items.iter().map(|e| e.format_compact()).collect::<Vec<_>>().join("\n");
                ok(format!("{}:\n\n{}", format_count(result.items.len(), result.total_count, offset), text))
            }
            Some("signature") => {
                if params.param_type.is_none() && params.return_type.is_none() && params.type_bound.is_none() {
                    return ok("Error: at least one of param_type, return_type, or type_bound required.".into());
                }
                let result = idx.search_signature(
                    params.param_type.as_deref(), params.return_type.as_deref(), params.type_bound.as_deref(),
                    params.name.as_deref().or(params.query.as_deref()), kind,
                    params.crate_name.as_deref(), params.module.as_deref(), offset, limit,
                );
                if result.items.is_empty() {
                    let mut desc = Vec::new();
                    if let Some(ref p) = params.param_type { desc.push(format!("param_type={}", p)); }
                    if let Some(ref r) = params.return_type { desc.push(format!("return_type={}", r)); }
                    if let Some(ref t) = params.type_bound { desc.push(format!("type_bound={}", t)); }
                    return ok(format!("No results for signature search: {}", desc.join(", ")));
                }
                let text: String = result.items.iter().map(|e| e.format_compact()).collect::<Vec<_>>().join("\n");
                ok(format!("{}:\n\n{}", format_count(result.items.len(), result.total_count, offset), text))
            }
            Some("trait") => {
                let name = params.name.or(params.query)
                    .ok_or_else(|| McpError::invalid_params("name is required for trait search", None))?;
                let traits = idx.lookup_trait(&name);
                let impls = idx.search_trait_impls(&name);
                if traits.is_empty() && impls.is_empty() {
                    let mut msg = format!("No trait or impls matching '{}'", name);
                    msg.push_str(&format_did_you_mean(&idx, &name));
                    return ok(msg);
                }
                let mut text = String::new();
                for t in &traits { text.push_str(&t.format_full()); text.push('\n'); }
                if !impls.is_empty() {
                    text.push_str(&format!("Implementations ({}):\n", impls.len()));
                    for i in &impls { text.push_str(&format!("  {}\n", i.format_compact())); }
                }
                ok(text)
            }
            Some("module") => {
                let name = params.query.or(params.name)
                    .ok_or_else(|| McpError::invalid_params("query (module path) is required", None))?;
                let (fns, types) = idx.browse_module(&name);
                if fns.is_empty() && types.is_empty() {
                    return ok(format!("No items in module '{}'", name));
                }
                let mut text = String::new();
                if !types.is_empty() {
                    text.push_str(&format!("Types ({}):\n", types.len()));
                    for t in &types { text.push_str(&format!("  {}\n", t.format_compact())); }
                    text.push('\n');
                }
                if !fns.is_empty() {
                    text.push_str(&format!("Functions ({}):\n", fns.len()));
                    for f in &fns { text.push_str(&format!("  {}\n", f.format_compact())); }
                }
                ok(text)
            }
            Some("modules") => {
                let modules = idx.list_modules();
                let total = idx.len() + idx.type_len();
                let mut crates: std::collections::BTreeMap<String, Vec<(String, usize)>> = std::collections::BTreeMap::new();
                for (path, count) in &modules {
                    let crate_name = path.split("::").next().unwrap_or(path);
                    let mod_name = path.splitn(2, "::").nth(1).unwrap_or("(root)");
                    crates.entry(crate_name.to_string()).or_default().push((mod_name.to_string(), *count));
                }
                let mut text = format!("{} items, {} modules\n\n", total, modules.len());
                for (cn, mods) in &crates {
                    let ct: usize = mods.iter().map(|(_, c)| c).sum();
                    let ml: Vec<String> = mods.iter().map(|(m, c)| format!("{}({})", m, c)).collect();
                    text.push_str(&format!("{} ({}): {}\n", cn, ct, ml.join(", ")));
                }
                ok(text)
            }
            Some("stats") => {
                let s = idx.stats();
                let mut text = format!(
                    "Total: {} functions, {} types, {} traits\nBy kind: {} spec, {} proof, {} exec\nProof debt: {} assume(false)\n",
                    s.total_functions, s.total_types, s.total_traits, s.spec, s.proof, s.exec, s.assume_false,
                );
                text.push_str("\nBy crate:\n");
                for (name, cs) in &s.by_crate {
                    let mut parts = vec![format!("{} fns", cs.functions)];
                    if cs.types > 0 { parts.push(format!("{} types", cs.types)); }
                    if cs.traits > 0 { parts.push(format!("{} traits", cs.traits)); }
                    if cs.assume_false > 0 { parts.push(format!("{} assume(false)", cs.assume_false)); }
                    text.push_str(&format!("  {}: {}\n", name, parts.join(", ")));
                }
                ok(text)
            }
            Some("source") => {
                let name = params.name.or(params.query)
                    .ok_or_else(|| McpError::invalid_params("name is required for source lookup", None))?;
                let fn_results = idx.lookup(&name);
                if fn_results.is_empty() {
                    let mut msg = format!("No function named '{}'", name);
                    msg.push_str(&format_did_you_mean(&idx, &name));
                    return ok(msg);
                }
                let mut sections = Vec::new();
                for e in &fn_results {
                    match std::fs::read_to_string(&e.file_path) {
                        Ok(contents) => {
                            let lines: Vec<&str> = contents.lines().collect();
                            let start = e.line.saturating_sub(1);
                            let end = e.end_line.min(lines.len());
                            sections.push(format!("//  {}:{}-{}\n{}", e.file_path, e.line, e.end_line, lines[start..end].join("\n")));
                        }
                        Err(err) => sections.push(format!("//  {}:{}-{} (could not read: {})", e.file_path, e.line, e.end_line, err)),
                    }
                }
                ok(sections.join("\n---\n"))
            }
            Some("dependencies") => {
                let name = params.name.or(params.query)
                    .ok_or_else(|| McpError::invalid_params("name is required for dependency search", None))?;
                let direction = params.direction.as_deref().unwrap_or("callers");
                match direction {
                    "callees" => {
                        let callees = idx.find_callees(&name);
                        if callees.is_empty() {
                            let mut msg = format!("'{}' calls no known functions", name);
                            msg.push_str(&format_did_you_mean(&idx, &name));
                            return ok(msg);
                        }
                        let mut sorted = callees;
                        sorted.sort();
                        ok(format!("'{}' calls {} functions:\n\n{}", name, sorted.len(), sorted.join("\n")))
                    }
                    _ => {
                        let callers = idx.find_callers(&name);
                        if callers.is_empty() {
                            let mut msg = format!("No callers found for '{}'", name);
                            msg.push_str(&format_did_you_mean(&idx, &name));
                            return ok(msg);
                        }
                        let text: String = callers.iter().map(|e| e.format_compact()).collect::<Vec<_>>().join("\n");
                        ok(format!("{} callers of '{}':\n\n{}", callers.len(), name, text))
                    }
                }
            }
            Some(other) => ok(format!(
                "Error: unknown scope '{}'. Valid: ensures, requires, body, doc, types, signature, trait, module, modules, dependencies, stats, source.",
                other
            )),
            None => {
                //  Default: batch lookup, exact lookup, or name search
                if let Some(names) = params.names {
                    if names.is_empty() { return ok("No names provided".into()); }
                    if names.len() > 10 { return ok("Max 10 names per call".into()); }
                    let mut sections = Vec::new();
                    for name in &names {
                        let fn_results = idx.lookup(name);
                        if !fn_results.is_empty() {
                            sections.push(fn_results.iter().map(|e| e.format_full()).collect::<Vec<_>>().join("\n"));
                            continue;
                        }
                        let type_results = idx.lookup_type(name);
                        if !type_results.is_empty() {
                            sections.push(type_results.iter().map(|e| e.format_full()).collect::<Vec<_>>().join("\n"));
                            continue;
                        }
                        sections.push(format!("'{}': not found", name));
                    }
                    ok(sections.join("\n---\n"))
                } else if let Some(name) = params.name {
                    //  Exact lookup
                    let fn_results = idx.lookup(&name);
                    if !fn_results.is_empty() {
                        return ok(fn_results.iter().map(|e| e.format_full()).collect::<Vec<_>>().join("\n"));
                    }
                    let type_results = idx.lookup_type(&name);
                    if !type_results.is_empty() {
                        return ok(type_results.iter().map(|e| e.format_full()).collect::<Vec<_>>().join("\n"));
                    }
                    let mut msg = format!("No function or type named '{}'", name);
                    msg.push_str(&format_did_you_mean(&idx, &name));
                    ok(msg)
                } else if let Some(query) = params.query {
                    //  Name substring search
                    let det_limit = if params.details {
                        params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(DEFAULT_RESULTS.min(10))
                    } else {
                        limit
                    };
                    let result = idx.search(&query, kind, params.crate_name.as_deref(), params.module.as_deref(), params.trait_only, offset, det_limit);

                    let mut text: String = result.items.iter()
                        .map(|e| if params.details { e.format_full() } else { e.format_compact() })
                        .collect::<Vec<_>>().join("\n");

                    if offset == 0 && result.total_count < 5 {
                        let fuzzy_limit = if result.items.is_empty() { 10 } else { DEFAULT_RESULTS.saturating_sub(result.items.len()) };
                        if fuzzy_limit > 0 {
                            let fuzzy = idx.search_fuzzy(&query, fuzzy_limit);
                            let existing: std::collections::HashSet<(&str, usize)> = result.items.iter().map(|e| (e.file_path.as_str(), e.line)).collect();
                            let fuzzy_new: Vec<_> = fuzzy.items.iter().filter(|e| !existing.contains(&(e.file_path.as_str(), e.line))).collect();
                            if !fuzzy_new.is_empty() {
                                text.push_str("\n\n--- fuzzy matches ---\n");
                                for e in &fuzzy_new { text.push_str(&format!("{}\n", e.format_compact())); }
                            }
                        }
                    }
                    if result.items.is_empty() && text.trim().is_empty() {
                        let mut msg = format!("No results for '{}'", query);
                        msg.push_str(&format_did_you_mean(&idx, &query));
                        return ok(msg);
                    }
                    let count = format_count(result.items.len(), result.total_count, offset);
                    ok(format!("{}:\n\n{}", count, text))
                } else {
                    ok("Error: provide query, name, or names (or set scope).".into())
                }
            }
        }
    }

    #[tool(description = "Read files and explore project structure.

No path (or directory path) → list directory contents (files and subdirectories).
File path alone → Returns a summary listing of top-level items (use statements, modules, functions, structs, traits, impls with their method names). This is NOT source code.
File path + name → Returns the full source code of that specific item (function, struct, impl method, etc.). Use 'Type::method' for impl methods.

Note: There is no way to retrieve an entire file's source at once. To view a complete file, you must call read separately for each item.")]
    pub async fn read(
        &self,
        Parameters(params): Parameters<ReadParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let path = params.path.unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string()
        });
        let meta = std::fs::metadata(&path)
            .map_err(|e| McpError::internal_error(format!("Cannot access {}: {}", path, e), None))?;

        if meta.is_dir() {
            let mut entries = Vec::new();
            let read_dir = std::fs::read_dir(&path)
                .map_err(|e| McpError::internal_error(format!("Cannot read {}: {}", path, e), None))?;
            for entry in read_dir.filter_map(|e| e.ok()) {
                let ft = entry.file_type().ok();
                let name = entry.file_name().to_string_lossy().to_string();
                if ft.map_or(false, |ft| ft.is_dir()) {
                    entries.push(format!("{}/", name));
                } else {
                    entries.push(name);
                }
            }
            entries.sort();
            if entries.is_empty() {
                Ok(CallToolResult::success(vec![Content::text("Empty directory.")]))
            } else {
                Ok(CallToolResult::success(vec![Content::text(entries.join("\n"))]))
            }
        } else if let Some(name) = params.name {
            let source = std::fs::read_to_string(&path)
                .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", path, e), None))?;
            match editor::read_fn(&source, &name) {
                Ok(text) => Ok(CallToolResult::success(vec![Content::text(text)])),
                Err(e) => Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))])),
            }
        } else {
            let source = std::fs::read_to_string(&path)
                .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", path, e), None))?;

            //  Mod statements as tree entries (not captured by parse_file)
            let mods: Vec<String> = source
                .lines()
                .filter(|l| {
                    let t = l.trim();
                    (t.starts_with("pub mod ") || t.starts_with("mod ")) && t.ends_with(';')
                })
                .map(|l| format!("- {}", l.trim()))
                .collect();

            let mut result = match editor::list_items_tree(&source) {
                Ok(tree) => tree,
                Err(_) => String::new(),
            };

            //  Insert mod entries after use statements but before other items
            if !mods.is_empty() {
                let mod_block = mods.join("\n");
                if result.is_empty() {
                    result = mod_block;
                } else {
                    //  Find the last "- use" line to insert mods after
                    let lines: Vec<&str> = result.lines().collect();
                    let last_use_idx = lines.iter().rposition(|l| l.starts_with("- use "));
                    if let Some(idx) = last_use_idx {
                        let before: Vec<&str> = lines[..=idx].to_vec();
                        let after: Vec<&str> = lines[idx + 1..].to_vec();
                        result = format!("{}\n{}\n{}", before.join("\n"), mod_block, after.join("\n"));
                    } else {
                        //  No use statements, prepend mods
                        result = format!("{}\n{}", mod_block, result);
                    }
                }
            }

            if result.is_empty() {
                result = "Empty file.".to_string();
            }

            Ok(CallToolResult::success(vec![Content::text(result)]))
        }
    }

    #[tool(description = "Add an item to a Verus source file.

use_path → add a use statement. Accepts full paths ('vstd::prelude::*') or short type names ('Ring') which auto-resolve from the index.
mod_name → add a `pub mod <name>;` declaration.
Otherwise → add a function. Provide either raw `source` or structured fields (name, kind, params, requires, ensures, body, etc.). Verus functions (spec/proof/exec) are auto-placed inside the verus! block. Set `after` to insert after a specific function.

inside → add the function inside a trait or impl block by name (e.g., 'MinimalPoly', 'Ring for SpecFieldExt'). Auto-indents to match existing methods.

Reports import changes (added/removed use statements) after mutation.")]
    pub async fn add(
        &self,
        Parameters(params): Parameters<AddParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }

        if let Some(ref use_path_raw) = params.use_path {
            //  --- Add use statement ---
            let mut use_path = use_path_raw.clone();
            if !use_path.contains("::") {
                self.wait_ready().await;
                let idx = self.index.read().map_err(|e| {
                    McpError::internal_error(format!("Lock error: {}", e), None)
                })?;
                let matches = idx.resolve_import(&use_path);
                match matches.len() {
                    0 => {
                        return Ok(CallToolResult::success(vec![Content::text(format!(
                            "No item named '{}' found in the index. Provide a full path instead.",
                            use_path
                        ))]));
                    }
                    1 => {
                        let (crate_name, module_path, item_name, _kind) = &matches[0];
                        let crate_mod = crate_name.replace('-', "_");
                        use_path = if module_path.is_empty() {
                            format!("{}::{}", crate_mod, item_name)
                        } else {
                            format!("{}::{}::{}", crate_mod, module_path, item_name)
                        };
                    }
                    _ => {
                        let mut msg = format!(
                            "'{}' is ambiguous. {} matches — call add again with one of:\n",
                            use_path,
                            matches.len()
                        );
                        for (crate_name, module_path, item_name, kind) in &matches {
                            let crate_mod = crate_name.replace('-', "_");
                            let full = if module_path.is_empty() {
                                format!("{}::{}", crate_mod, item_name)
                            } else {
                                format!("{}::{}::{}", crate_mod, module_path, item_name)
                            };
                            msg.push_str(&format!("  [{}] {}\n", kind, full));
                        }
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }
                }
            }
            let source = std::fs::read_to_string(&params.file)
                .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", params.file, e), None))?;
            match editor::add_use(&source, &use_path) {
                Ok(new_source) => {
                    std::fs::write(&params.file, &new_source)
                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                    let diff = Self::uses_diff(&source, &new_source);
                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Added: use {};{}",
                        use_path, diff
                    ))]))
                }
                Err(e) => Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))])),
            }
        } else if let Some(ref mod_name) = params.mod_name {
            //  --- Add pub mod statement ---
            let source = std::fs::read_to_string(&params.file)
                .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", params.file, e), None))?;
            let mod_line = format!("pub mod {};", mod_name);
            if source
                .lines()
                .any(|l| l.trim() == mod_line || l.trim() == format!("mod {};", mod_name))
            {
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "Module '{}' already declared.",
                    mod_name
                ))]));
            }
            //  Find insertion point: after last mod decl, else after last use, else at top
            let mut insert_pos = 0usize;
            let mut pos = 0usize;
            for line in source.split('\n') {
                pos += line.len() + 1;
                let t = line.trim();
                if (t.starts_with("pub mod ") || t.starts_with("mod ")) && t.ends_with(';') {
                    insert_pos = pos.min(source.len());
                }
            }
            if insert_pos == 0 {
                pos = 0;
                for line in source.split('\n') {
                    pos += line.len() + 1;
                    if line.trim().starts_with("use ") {
                        insert_pos = pos.min(source.len());
                    }
                }
            }
            let new_source = format!(
                "{}{}{}",
                &source[..insert_pos],
                format!("{}\n", mod_line),
                &source[insert_pos..]
            );
            std::fs::write(&params.file, &new_source)
                .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
            Ok(CallToolResult::success(vec![Content::text(format!(
                "Added: {}",
                mod_line
            ))]))
        } else {
            //  --- Add function ---
            let fn_source = match assemble_fn(&params.spec) {
                Ok(s) => s,
                Err(e) => {
                    return Ok(CallToolResult::success(vec![Content::text(format!(
                        "Error: {}",
                        e
                    ))]))
                }
            };
            let source = std::fs::read_to_string(&params.file)
                .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", params.file, e), None))?;

            //  Extract function name to check for existing
            let fn_name: Option<String> = params.spec.name.clone().or_else(|| {
                editor::parse_file(&fn_source).ok().and_then(|items| {
                    items.functions.first().map(|f| f.qualified_name.clone())
                })
            });

            //  If function already exists, replace it (replace_fn uses find_fn, so only matches functions)
            if let Some(ref name) = fn_name {
                if let Ok(new_source) = editor::replace_fn(&source, name, &fn_source) {
                    std::fs::write(&params.file, &new_source)
                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                    let diff = Self::uses_diff(&source, &new_source);
                    return Ok(CallToolResult::success(vec![Content::text(format!(
                        "Replaced existing '{}' in {}{}",
                        name, params.file, diff
                    ))]));
                }
            }

            let result = if let Some(ref inside) = params.inside {
                editor::add_fn_inside(&source, &fn_source, inside, params.after.as_deref())
            } else {
                editor::add_fn(&source, &fn_source, params.after.as_deref())
            };
            match result {
                Ok(new_source) => {
                    std::fs::write(&params.file, &new_source)
                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                    let diff = Self::uses_diff(&source, &new_source);
                    let label = fn_name.as_deref().unwrap_or("function");
                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Added '{}' to {}{}",
                        label, params.file, diff
                    ))]))
                }
                Err(e) => Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))])),
            }
        }
    }

    #[tool(description = "Remove an item from a Verus source file.

name → remove a function (or 'Type::method' for impl methods). Also removes its doc comment.
use_path → remove a use statement by substring match.
mod_name → remove a `pub mod <name>;` or `mod <name>;` declaration.

Exactly one of name, use_path, or mod_name is required. Reports import changes after mutation.")]
    pub async fn remove(
        &self,
        Parameters(params): Parameters<RemoveParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let source = std::fs::read_to_string(&params.file)
            .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", params.file, e), None))?;

        if let Some(ref name) = params.name {
            match editor::delete_fn(&source, name) {
                Ok(new_source) => {
                    std::fs::write(&params.file, &new_source)
                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                    let diff = Self::uses_diff(&source, &new_source);
                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Deleted '{}' from {}{}",
                        name, params.file, diff
                    ))]))
                }
                Err(e) => Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))])),
            }
        } else if let Some(ref use_path) = params.use_path {
            match editor::remove_use(&source, use_path) {
                Ok(new_source) => {
                    std::fs::write(&params.file, &new_source)
                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                    let diff = Self::uses_diff(&source, &new_source);
                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Removed use statement matching '{}'{}",
                        use_path, diff
                    ))]))
                }
                Err(e) => Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))])),
            }
        } else if let Some(ref mod_name) = params.mod_name {
            let patterns = [
                format!("pub mod {};", mod_name),
                format!("mod {};", mod_name),
            ];
            let mut found = false;
            let mut new_lines: Vec<&str> = Vec::new();
            for line in source.lines() {
                if !found && patterns.iter().any(|p| line.trim() == *p) {
                    found = true;
                    continue;
                }
                new_lines.push(line);
            }
            if !found {
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "Error: no mod declaration for '{}' found.",
                    mod_name
                ))]));
            }
            let mut new_source = new_lines.join("\n");
            if source.ends_with('\n') && !new_source.ends_with('\n') {
                new_source.push('\n');
            }
            std::fs::write(&params.file, &new_source)
                .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
            Ok(CallToolResult::success(vec![Content::text(format!(
                "Removed mod declaration for '{}'",
                mod_name
            ))]))
        } else {
            //  Check if the model likely intended to pass name but sent null
            Ok(CallToolResult::success(vec![Content::text(
                "Error: `name`, `use_path`, or `mod_name` is required but all were null/empty. \
                 Pass a non-null value — e.g. name=\"my_function\" to remove a function."
                    .to_string(),
            )]))
        }
    }

    #[tool(description = "Edit a function or use statement via scoped string replacement.

With name: finds old_string within that function only and replaces with new_string. old_string must appear exactly once. Supports 'Type::method' for impl methods.

Without name: auto-detects the containing function. If old_string spans multiple functions or isn't inside one, falls back to file-level matching.

Wildcards in old_string:
- A line with just `...` matches the smallest span of text (skips lines you don't want to type out).
- A line with just `{ ... }` matches a full brace-balanced block (from `{` to matching `}`).

Use this for surgical edits — changing a requires clause, fixing a body statement, renaming a parameter, updating imports, etc.")]
    pub async fn edit(
        &self,
        Parameters(params): Parameters<EditParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(msg) = self.require_standalone() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let source = std::fs::read_to_string(&params.file)
            .map_err(|e| McpError::internal_error(format!("Failed to read {}: {}", params.file, e), None))?;

        //  Check if old_string is a use statement edit
        let old_trimmed = params.old_string.trim();
        let is_use_edit = old_trimmed.starts_with("use ") || old_trimmed.starts_with("pub use ");

        if is_use_edit {
            let count = source.matches(&params.old_string).count();
            if count == 0 {
                return Ok(CallToolResult::success(vec![Content::text(
                    "Error: old_string not found in file."
                )]));
            }
            if count > 1 {
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "Error: old_string is ambiguous: found {} matches.", count
                ))]));
            }
            let new_source = source.replacen(&params.old_string, &params.new_string, 1);
            std::fs::write(&params.file, &new_source)
                .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
            let diff = Self::uses_diff(&source, &new_source);
            Ok(CallToolResult::success(vec![Content::text(format!(
                "Edited use statement in {}{}",
                params.file, diff
            ))]))
        } else {
            let name: String = match params.name {
                Some(ref n) if !n.is_empty() => n.clone(),
                _ => {
                    //  Auto-detect: find which function contains old_string
                    match editor::find_containing_fn(&source, &params.old_string) {
                        Ok(matches) if matches.len() == 1 => matches[0].clone(),
                        Ok(matches) if matches.is_empty() => {
                            //  No function contains old_string — try file-level edit
                            //  (handles multi-function edits and ellipsis patterns)
                            match editor::edit_file(&source, &params.old_string, &params.new_string) {
                                Ok(new_source) => {
                                    std::fs::write(&params.file, &new_source)
                                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                                    let diff = Self::uses_diff(&source, &new_source);
                                    return Ok(CallToolResult::success(vec![Content::text(format!(
                                        "Edited {}{}",
                                        params.file, diff
                                    ))]));
                                }
                                Err(e) => {
                                    return Ok(CallToolResult::success(vec![Content::text(format!(
                                        "Error: {}", e
                                    ))]));
                                }
                            }
                        }
                        Ok(matches) => {
                            return Ok(CallToolResult::success(vec![Content::text(format!(
                                "Error: old_string found in {} functions. Pass `name` to disambiguate: {}",
                                matches.len(),
                                matches.join(", ")
                            ))]));
                        }
                        Err(e) => {
                            return Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))]));
                        }
                    }
                }
            };
            let name = name.as_str();
            match editor::edit_fn(&source, name, &params.old_string, &params.new_string) {
                Ok(new_source) => {
                    std::fs::write(&params.file, &new_source)
                        .map_err(|e| McpError::internal_error(format!("Failed to write {}: {}", params.file, e), None))?;
                    let diff = Self::uses_diff(&source, &new_source);
                    //  Extract edited function source + start line for UI context
                    let fn_context = editor::read_fn(&new_source, name)
                        .ok()
                        .and_then(|fn_src| {
                            //  Find the start line (1-indexed)
                            let byte_offset = new_source.find(fn_src.lines().next().unwrap_or(""))?;
                            let start_line = new_source[..byte_offset].matches('\n').count() + 1;
                            Some(format!("\n@@fn_start={}\n{}\n@@fn_end", start_line, fn_src))
                        })
                        .unwrap_or_default();
                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Edited function '{}' in {}{}{}",
                        name, params.file, diff, fn_context
                    ))]))
                }
                Err(e) => Ok(CallToolResult::success(vec![Content::text(format!("Error: {}", e))])),
            }
        }
    }
}

///  Tools only available in standalone mode.
const STANDALONE_ONLY: &[&str] = &["find", "read", "add", "remove", "edit"];

///  Tools hidden in standalone mode (replaced by unified tools above).
const HIDDEN_IN_STANDALONE: &[&str] = &[
    "search",
    "search_ensures",
    "search_requires",
    "search_signature",
    "search_body",
    "search_doc",
    "search_types",
    "search_trait",
    "browse_module",
    "lookup",
    "lookup_source",
    "batch_lookup",
    "find_dependencies",
    "list_modules",
    "context_list",
    "context_activate",
    "stats",
];

impl ServerHandler for VerusMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("verus-mcp", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Verus proof index server. Search spec/proof/exec functions, \
                 look up lemmas by name, search requires/ensures clauses.",
            )
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let standalone = crate::STANDALONE.load(std::sync::atomic::Ordering::Relaxed);
        let tools: Vec<Tool> = self
            .tool_router
            .list_all()
            .into_iter()
            .filter(|t| {
                let name = t.name.as_ref();
                if standalone {
                    !HIDDEN_IN_STANDALONE.contains(&name)
                } else {
                    !STANDALONE_ONLY.contains(&name)
                }
            })
            .collect();
        std::future::ready(Ok(ListToolsResult {
            tools,
            meta: None,
            next_cursor: None,
        }))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let tcc = ToolCallContext::new(self, request, context);
        async move { self.tool_router.call(tcc).await }
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    //  ─── pick_verus_summary ─────────────────────────────────────────────────

    #[test]
    fn single_summary_module_level() {
        //  --verify-module emits exactly one summary line.
        let combined = "verification results:: 0 verified, 0 errors, 96 cached \
                        (partial verification with `--verify-*`)\n\
                        Compiling verus-rational v0.1.0\n\
                        Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.20s\n";
        assert_eq!(pick_verus_summary(combined), Some((0, 0, 96)));
    }

    #[test]
    fn dual_summary_full_crate_picks_real_one() {
        //  Full-crate `cargo verus verify` emits a verify pass summary AND
        //  a trailing build pass summary that's always "0 verified, 0 errors".
        //  We must pick the verify pass (the one with `cached`), not the
        //  trailing empty summary. This was the original bug.
        let combined = "verification results:: 0 verified, 0 errors, 278 cached\n\
                        verification results:: 0 verified, 0 errors\n\
                        Compiling verus-mandelbrot v0.1.0\n\
                        Finished `dev` profile in 6.12s\n";
        assert_eq!(pick_verus_summary(combined), Some((0, 0, 278)));
    }

    #[test]
    fn dual_summary_with_real_verification_work() {
        //  Some functions need re-verification (not all cached). The non-empty
        //  summary still wins.
        let combined = "verification results:: 12 verified, 0 errors, 266 cached\n\
                        verification results:: 0 verified, 0 errors\n";
        assert_eq!(pick_verus_summary(combined), Some((12, 0, 266)));
    }

    #[test]
    fn dependency_modified_then_rerun_output() {
        //  When a dependency is modified, `has_dependency_compilation`
        //  triggers a rerun. The rerun output (which `parse_verus_output`
        //  ultimately sees) contains only the target's summary because deps
        //  are now cached and not re-invoked. Stats must come through
        //  unchanged after the rerun.
        let stdout = "verification results:: 4 verified, 0 errors, 270 cached\n\
                      verification results:: 0 verified, 0 errors\n";
        let stderr = "    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.45s\n";
        let combined = format!("{}\n{}", stdout, stderr);
        assert_eq!(pick_verus_summary(&combined), Some((4, 0, 270)));
    }

    #[test]
    fn dependency_summary_present_does_not_swallow_target() {
        //  Defensive: if a dep summary somehow leaked through (e.g., the rerun
        //  guard failed for some reason), max_by_key would prefer whichever
        //  has more total work. We document this behavior so a future change
        //  to the rerun guard doesn't silently regress: when both are present,
        //  the *larger* summary wins. In practice the rerun guard prevents
        //  this case, but the test pins the behavior so we know it's defined.
        //
        //  Here the target has more work than the dep:
        let combined = "verification results:: 5 verified, 0 errors, 95 cached\n\
                        verification results:: 50 verified, 0 errors, 600 cached\n\
                        verification results:: 0 verified, 0 errors\n";
        assert_eq!(pick_verus_summary(combined), Some((50, 0, 600)));
    }

    #[test]
    fn errors_present_picks_the_error_summary() {
        //  When verification fails, the summary has errors > 0. The empty
        //  trailing summary may or may not be present; either way the
        //  errored summary should win since it has more total work.
        let combined = "verification results:: 18 verified, 3 errors, 250 cached\n\
                        verification results:: 0 verified, 0 errors\n";
        assert_eq!(pick_verus_summary(combined), Some((18, 3, 250)));
    }

    #[test]
    fn no_summary_returns_none() {
        let combined = "Compiling verus-rational v0.1.0\n\
                        error[E0308]: mismatched types\n\
                        Finished with errors\n";
        assert_eq!(pick_verus_summary(combined), None);
    }

    #[test]
    fn summary_without_cached_field() {
        //  Old verus versions or `--verify-*` outputs may omit the cached
        //  count entirely. Should still parse with cached=0.
        let combined = "verification results:: 42 verified, 0 errors\n";
        assert_eq!(pick_verus_summary(combined), Some((42, 0, 0)));
    }

    //  ─── has_dependency_compilation ─────────────────────────────────────────

    #[test]
    fn dep_compilation_detected_when_other_crate_compiles() {
        let stderr = "   Compiling verus-bigint v0.1.0\n\
                      Compiling verus-rational v0.1.0\n\
                      Finished\n";
        assert!(has_dependency_compilation(stderr, "verus-rational"));
    }

    #[test]
    fn dep_compilation_not_detected_when_only_target_compiles() {
        let stderr = "   Compiling verus-rational v0.1.0\n\
                      Finished\n";
        assert!(!has_dependency_compilation(stderr, "verus-rational"));
    }

    #[test]
    fn dep_compilation_underscore_target_name() {
        //  cargo prints the underscore form for some crates; the check should
        //  recognize both forms as the target.
        let stderr = "   Compiling verus_rational v0.1.0\n\
                      Finished\n";
        assert!(!has_dependency_compilation(stderr, "verus-rational"));
    }

    #[test]
    fn dep_compilation_detected_when_target_also_compiles() {
        //  This is the realistic "dep modified" case: a dep was changed, so
        //  cargo recompiles BOTH the dep and the target. The function should
        //  return true so the caller reruns to get clean per-target output.
        let stderr = "   Compiling verus-bigint v0.1.0\n\
                      Compiling verus-rational v0.1.0\n\
                      Finished\n";
        assert!(has_dependency_compilation(stderr, "verus-rational"));
    }

    #[test]
    fn dep_compilation_empty_stderr() {
        //  Fully cached run — no Compiling lines at all.
        assert!(!has_dependency_compilation("", "verus-rational"));
        assert!(!has_dependency_compilation(
            "    Finished `dev` profile in 0.12s\n",
            "verus-rational",
        ));
    }
}
