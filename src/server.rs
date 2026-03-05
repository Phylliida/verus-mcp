use crate::index::{Index, Matcher, DEFAULT_RESULTS, MAX_RESULTS};
use crate::indexer;
use crate::types::FnKind;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use rmcp::schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{Arc, RwLock};
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
pub struct DependencyParams {
    /// Function name to find dependencies for
    pub name: String,
    /// Direction: "callers" (who calls this function) or "callees" (what this function calls). Default: "callers"
    pub direction: Option<String>,
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
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl VerusMcpServer {
    pub fn new(index: Arc<RwLock<Index>>, ready: watch::Receiver<bool>) -> Self {
        Self {
            index,
            ready,
            tool_router: Self::tool_router(),
        }
    }

    /// Wait for the initial index build to complete (no-op once ready).
    async fn wait_ready(&self) {
        let mut rx = self.ready.clone();
        // wait_for returns immediately if the value already satisfies the predicate
        let _ = rx.wait_for(|&v| v).await;
    }

    #[tool(description = "Search Verus proof/spec/exec functions by name substring. Returns matching function signatures with module paths and file locations.")]
    pub async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<CallToolResult, McpError> {
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
        for name in &params.names {
            let fn_results = idx.lookup(name);
            if !fn_results.is_empty() {
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

        Ok(CallToolResult::success(vec![Content::text(
            sections.join("\n---\n"),
        )]))
    }

    #[tool(description = "Search within ensures clauses of Verus functions. Useful for finding lemmas that prove a specific property. Query supports regex (e.g., 'div.*mul.*eqv'); falls back to substring if not valid regex.")]
    pub async fn search_ensures(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        self.wait_ready().await;
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let limit = params.limit.map(|l| l.min(MAX_RESULTS)).unwrap_or(MAX_RESULTS);
        let offset = params.offset.unwrap_or(0);
        let kind = params.kind.as_deref().and_then(parse_kind);
        let result = idx.search_ensures(&params.query, params.crate_name.as_deref(), params.module.as_deref(), params.name.as_deref(), kind, offset, limit);

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

    #[tool(description = "Rebuild the index from disk. Use after editing Verus source files. Only re-parses files that changed since the last index.")]
    pub async fn reindex(&self) -> Result<CallToolResult, McpError> {
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
