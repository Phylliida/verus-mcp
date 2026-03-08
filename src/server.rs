use crate::index::{Index, Matcher, DEFAULT_RESULTS, MAX_RESULTS};
use crate::indexer;
use crate::types::FnKind;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchParams {
    /// Name substring to search for
    pub query: String,
    /// Filter by function kind: "spec", "proof", or "exec"
    pub kind: Option<String>,
    /// Filter by crate name
    pub crate_name: Option<String>,
    /// Filter by module path substring
    pub module: Option<String>,
    /// Only show trait axioms/methods
    #[serde(default)]
    pub trait_only: bool,
    /// When true, return full signatures with requires/ensures (default limit drops to 10)
    #[serde(default)]
    pub details: bool,
    /// Max results to return (default 50, or 10 when details=true)
    pub limit: Option<usize>,
    /// Skip first N results for pagination (default 0)
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LookupParams {
    /// Exact function name to look up
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct BatchLookupParams {
    /// List of function/type names to look up (max 10)
    pub names: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ClauseSearchParams {
    /// Substring to search within requires/ensures clauses
    pub query: String,
    /// Filter by crate name
    pub crate_name: Option<String>,
    /// Filter by module path substring
    pub module: Option<String>,
    /// Filter by function name substring
    pub name: Option<String>,
    /// Filter by function kind: "spec", "proof", or "exec"
    pub kind: Option<String>,
    /// Max results to return (default 50)
    pub limit: Option<usize>,
    /// Skip first N results for pagination (default 0)
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SignatureSearchParams {
    /// Substring to match against parameter types (e.g., "Vec2", "Point3", "Seq")
    pub param_type: Option<String>,
    /// Substring to match against return type (e.g., "bool", "Sign")
    pub return_type: Option<String>,
    /// Substring to match against type parameter bounds (e.g., "OrderedRing", "Field")
    pub type_bound: Option<String>,
    /// Optional name substring filter to combine with type filters
    pub name: Option<String>,
    /// Filter by function kind: "spec", "proof", or "exec"
    pub kind: Option<String>,
    /// Filter by crate name
    pub crate_name: Option<String>,
    /// Filter by module path substring
    pub module: Option<String>,
    /// Max results to return (default 50)
    pub limit: Option<usize>,
    /// Skip first N results for pagination (default 0)
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CheckParams {
    /// Crate directory name (e.g., "verus-geometry", "verus-topology")
    pub crate_name: String,
    /// Optional: verify only this module. Accepts a file path (e.g., "src/runtime/polygon.rs")
    /// or module path (e.g., "runtime::polygon"). Bypasses check.sh and runs cargo verus directly.
    pub module: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProfileParams {
    /// Crate directory name (e.g., "verus-geometry", "verus-topology")
    pub crate_name: String,
    /// Optional: profile only this module. Accepts a file path or module path.
    pub module: Option<String>,
    /// Number of top functions to show (default: 25)
    pub top_n: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DependencyParams {
    /// Function name to find dependencies for
    pub name: String,
    /// Direction: "callers" (who calls this function) or "callees" (what this function calls). Default: "callers"
    pub direction: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ContextActivateParams {
    /// Context name to activate or create. Omit to list recent contexts.
    pub name: Option<String>,
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
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::PathBuf::from(home)
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

/// Convert a file path or module path to a Verus --verify-module argument.
/// Verus uses crate-local module paths (e.g., "runtime::polygon", not "verus_geometry::runtime::polygon").
/// Accepts: "src/runtime/polygon.rs", "runtime/polygon.rs", "runtime::polygon"
fn to_verify_module(crate_name: &str, input: &str) -> String {
    let crate_mod = crate_name.replace('-', "_");

    // If it already looks like a module path (has :: and no /), strip crate prefix if present
    if input.contains("::") && !input.contains('/') {
        let stripped = input
            .strip_prefix(&format!("{}::", crate_mod))
            .or_else(|| input.strip_prefix("crate::"))
            .unwrap_or(input);
        return stripped.to_string();
    }

    // File path: strip src/ prefix and .rs suffix, convert / to ::
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

/// Check if a "could not find module" error came from a dependency crate, not the target.
/// This happens when `--verify-module` flags after `--` are passed to ALL crate compilations.
fn is_dependency_module_error(output: &str, target_pkg: &str) -> bool {
    let mut last_compiling_crate = None;
    for line in output.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("Compiling ") {
            if let Some(name) = rest.split_whitespace().next() {
                last_compiling_crate = Some(name.to_string());
            }
        }
        if line.contains("could not find module")
            && (line.contains("--verify-module") || line.contains("--verify-only-module"))
        {
            if let Some(ref crate_name) = last_compiling_crate {
                if crate_name != target_pkg {
                    return true;
                }
            }
        }
    }
    false
}

/// Run a bash script with a 10-minute timeout. Returns the process output.
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

/// Build the bash script for `cargo verus verify` (check mode).
fn build_check_script(
    default_verus_root: &std::path::Path,
    pkg: &str,
    module_flag: &str,
) -> String {
    format!(
        r#"set -euo pipefail
VERUS_ROOT="${{VERUS_ROOT:-{default_verus_root}}}"
VERUS_SOURCE="$VERUS_ROOT/source"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TOOLCHAIN="1.93.0-aarch64-apple-darwin" ;;
  Darwin-x86_64) TOOLCHAIN="1.93.0-x86_64-apple-darwin" ;;
  *)             TOOLCHAIN="1.93.0-x86_64-unknown-linux-gnu" ;;
esac
export PATH="$VERUS_SOURCE/target-verus/release:$PATH"
export VERUS_Z3_PATH="$VERUS_SOURCE/z3"
export RUSTUP_TOOLCHAIN="$TOOLCHAIN"
cargo verus verify --manifest-path Cargo.toml -p {pkg} -- {module_flag}--triggers-mode silent
"#,
        default_verus_root = default_verus_root.display(),
        pkg = pkg,
        module_flag = module_flag,
    )
}

/// Build the bash preamble for `cargo verus verify` (profile mode).
/// Returns everything up to (and including) the `python3 ... <<'PYEOF'` line.
fn build_profile_preamble(
    default_verus_root: &std::path::Path,
    pkg: &str,
    module_flag: &str,
    top_n: usize,
) -> String {
    format!(
        r#"set -euo pipefail
unset RUSTFLAGS
unset CARGO_ENCODED_RUSTFLAGS
VERUS_ROOT="${{VERUS_ROOT:-{default_verus_root}}}"
VERUS_SOURCE="$VERUS_ROOT/source"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TOOLCHAIN="1.93.0-aarch64-apple-darwin" ;;
  Darwin-x86_64) TOOLCHAIN="1.93.0-x86_64-apple-darwin" ;;
  *)             TOOLCHAIN="1.93.0-x86_64-unknown-linux-gnu" ;;
esac
export PATH="$VERUS_SOURCE/target-verus/release:$PATH"
export VERUS_Z3_PATH="$VERUS_SOURCE/z3"
export RUSTUP_TOOLCHAIN="$TOOLCHAIN"

JSON_FILE="$(mktemp)"
trap 'rm -f "$JSON_FILE"' EXIT

cargo verus verify --manifest-path Cargo.toml -p {pkg} \
  -- {module_flag}--output-json --time-expanded --triggers-mode silent > "$JSON_FILE" || true

python3 - "$JSON_FILE" "{top_n}" <<'PYEOF'
"#,
        default_verus_root = default_verus_root.display(),
        pkg = pkg,
        module_flag = module_flag,
        top_n = top_n,
    )
}

/// Format "Did you mean:" suggestions, or empty string if none found.
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

/// Format a result count line with pagination info.
/// "5 results", "5 of 23 results", or "results 51-75 of 100".
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

    /// Wait for the initial index build to complete (no-op once ready).
    async fn wait_ready(&self) {
        let mut rx = self.ready.clone();
        // wait_for returns immediately if the value already satisfies the predicate
        let _ = rx.wait_for(|&v| v).await;
    }

    /// Check if a context is active. Returns a gate message if not.
    fn require_context(&self) -> Option<String> {
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

    /// Capture item names into the active context (no-op if no context active).
    /// Duplicates are moved to the end (most recently fetched last).
    fn capture_names(&self, names: impl IntoIterator<Item = impl AsRef<str>>) {
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
        // Trim to last 250 items to avoid context window limits on replay
        const MAX_CONTEXT_ITEMS: usize = 250;
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
        let recent = list_contexts();
        let ctx_guard = self.context.lock().unwrap();
        let active_info = match &ctx_guard.active {
            Some(name) => format!("Active: {} ({} items)\n\n", name, ctx_guard.items.len()),
            None => String::new(),
        };
        drop(ctx_guard);

        // Mark as listed so context_activate is unblocked
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
        // Gate: must call context_list first
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
                // Resume: load items, set active
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
                // Create new context
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

        // Auto-capture to context when 1-2 results
        if result.total_count >= 1 && result.total_count <= 2 {
            self.capture_names(result.items.iter().map(|e| &e.name));
        }

        let mut text: String = result
            .items
            .iter()
            .map(|e| if params.details { e.format_full() } else { e.format_compact() })
            .collect::<Vec<_>>()
            .join("\n");

        // When substring results are few and no offset, append fuzzy matches
        if offset == 0 && result.total_count < 5 {
            let fuzzy_limit = if result.items.is_empty() { 10 } else { DEFAULT_RESULTS.saturating_sub(result.items.len()) };
            if fuzzy_limit > 0 {
                let fuzzy = idx.search_fuzzy(&params.query, fuzzy_limit);
                // Filter out items already in substring results
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

            // Note active filters and check if removing them helps
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

        // Fallback: search types
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
            // Read source lines from disk
            match std::fs::read_to_string(&e.file_path) {
                Ok(contents) => {
                    let lines: Vec<&str> = contents.lines().collect();
                    let start = e.line.saturating_sub(1); // 1-indexed to 0-indexed
                    let end = e.end_line.min(lines.len());
                    let source: String = lines[start..end]
                        .join("\n");
                    sections.push(format!(
                        "// {}:{}-{}\n{}",
                        e.file_path, e.line, e.end_line, source
                    ));
                }
                Err(err) => {
                    sections.push(format!(
                        "// {}:{}-{} (could not read: {})",
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

        // Auto-capture to context when 1-2 results
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
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);

        // Search both functions and types
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
        if let Some(msg) = self.require_context() {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let modules = idx.list_modules();
        let total = idx.len() + idx.type_len();

        // Group modules by crate
        let mut crates: std::collections::BTreeMap<String, Vec<(String, usize)>> =
            std::collections::BTreeMap::new();
        for (path, count) in &modules {
            // module_path is like "verus_algebra::ring_lemmas" — crate is first segment
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
                // "callers" (default)
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

    #[tool(description = "Run Verus verification on a crate. Returns summary on success, or error diagnostics on failure. Timeout: 10 minutes.")]
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

        let default_verus_root = workspace.join("verus");
        let module_flag = match params.module {
            Some(ref m) => format!("--verify-module {} ", to_verify_module(&params.crate_name, m)),
            None => String::new(),
        };
        let script = build_check_script(&default_verus_root, &params.crate_name, &module_flag);
        let output = match run_bash_script(&script, &crate_dir).await {
            Ok(output) => output,
            Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
        };
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        // If --verify-module hit a dependency that doesn't have that module,
        // fall back to full crate verification.
        if !module_flag.is_empty() && is_dependency_module_error(&combined, &params.crate_name) {
            let fallback = build_check_script(&default_verus_root, &params.crate_name, "");
            let output = match run_bash_script(&fallback, &crate_dir).await {
                Ok(output) => output,
                Err(msg) => return Ok(CallToolResult::success(vec![Content::text(msg)])),
            };
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return self.parse_verus_output(
                &params.crate_name,
                &combined,
                Some("(--verify-module bypassed: dependency recompilation detected, full crate verified)"),
            );
        }

        self.parse_verus_output(&params.crate_name, &combined, None)
    }

    /// Parse cargo verus output into a structured result.
    fn parse_verus_output(
        &self,
        crate_name: &str,
        combined: &str,
        note: Option<&str>,
    ) -> Result<CallToolResult, McpError> {
        let note_prefix = note.map(|n| format!("{}\n\n", n)).unwrap_or_default();
        let summary_re =
            regex::Regex::new(r"verification results::\s*(\d+) verified,\s*(\d+) errors")
                .unwrap();

        if let Some(caps) = summary_re.captures_iter(combined).last() {
            let verified: usize = caps[1].parse().unwrap_or(0);
            let errors: usize = caps[2].parse().unwrap_or(0);

            if errors == 0 {
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "{}{}: {} verified, 0 errors",
                    note_prefix, crate_name, verified
                ))]));
            }

            // Extract error blocks
            let mut error_blocks: Vec<String> = Vec::new();
            let mut current_block: Vec<String> = Vec::new();
            let mut in_error = false;

            for line in combined.lines() {
                if (line.starts_with("error:") || line.starts_with("error["))
                    && !line.contains("Verus verification summary")
                {
                    if in_error && !current_block.is_empty() {
                        error_blocks.push(current_block.join("\n"));
                        current_block.clear();
                    }
                    in_error = true;
                    current_block.push(line.to_string());
                } else if in_error {
                    let trimmed = line.trim_start();
                    if trimmed.is_empty() {
                        error_blocks.push(current_block.join("\n"));
                        current_block.clear();
                        in_error = false;
                    } else if trimmed.starts_with('|')
                        || trimmed.starts_with("-->")
                        || trimmed.starts_with("note:")
                        || trimmed.starts_with("help:")
                        || trimmed.starts_with("=")
                        || line.starts_with(' ')
                    {
                        current_block.push(line.to_string());
                    } else {
                        error_blocks.push(current_block.join("\n"));
                        current_block.clear();
                        in_error = false;
                    }
                }
            }
            if !current_block.is_empty() {
                error_blocks.push(current_block.join("\n"));
            }

            // Deduplicate (check.sh cats the log on error, producing duplicates)
            let mut seen = std::collections::HashSet::new();
            error_blocks.retain(|b| seen.insert(b.clone()));

            let mut text = format!("{}{}", note_prefix, error_blocks.join("\n\n"));
            text.push_str(&format!(
                "\n\n{}: {} verified, {} errors",
                crate_name, verified, errors
            ));
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        // No summary found — likely a build error. Return last 50 lines.
        let lines: Vec<&str> = combined.lines().collect();
        let start = lines.len().saturating_sub(50);
        let tail = lines[start..].join("\n");
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{}No verification summary found (build error?)\n\n{}",
            note_prefix, tail
        ))]))
    }

    #[tool(description = "Profile Verus verification: per-function SMT time and rlimit breakdown. Returns sorted table of hottest functions and per-module summary. Timeout: 10 minutes.")]
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

        let default_verus_root = workspace.join("verus");
        let module_flag = match params.module {
            Some(ref m) => format!("--verify-module {} ", to_verify_module(&params.crate_name, m)),
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

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if stdout.trim().is_empty() {
            // Check if a dependency failed due to --verify-module flag
            if !module_flag.is_empty() && is_dependency_module_error(&stderr, &params.crate_name) {
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
                        "(--verify-module bypassed: dependency recompilation detected, full crate profiled)\n\n{}",
                        stdout
                    ))]));
                }
                // Retry also failed
                let lines: Vec<&str> = stderr.lines().collect();
                let start = lines.len().saturating_sub(50);
                let tail = lines[start..].join("\n");
                return Ok(CallToolResult::success(vec![Content::text(format!(
                    "Profile failed\n\n{}", tail
                ))]));
            }

            // Python or cargo failed — show stderr
            let lines: Vec<&str> = stderr.lines().collect();
            let start = lines.len().saturating_sub(50);
            let tail = lines[start..].join("\n");
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Profile failed\n\n{}", tail
            ))]));
        }

        Ok(CallToolResult::success(vec![Content::text(stdout.to_string())]))
    }

    #[tool(description = "Rebuild the index from disk. Use after editing Verus source files. Only re-parses files that changed since the last index.")]
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
}

#[tool_handler]
impl ServerHandler for VerusMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("verus-mcp", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Verus proof index server. Search spec/proof/exec functions, \
                 look up lemmas by name, search requires/ensures clauses.",
            )
    }
}
