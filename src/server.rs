use crate::index::Index;
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
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LookupParams {
    /// Exact function name to look up
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ClauseSearchParams {
    /// Substring to search within requires/ensures clauses
    pub query: String,
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
}

fn parse_kind(s: &str) -> Option<FnKind> {
    match s.to_lowercase().as_str() {
        "spec" => Some(FnKind::Spec),
        "proof" => Some(FnKind::Proof),
        "exec" => Some(FnKind::Exec),
        _ => None,
    }
}

#[derive(Clone)]
pub struct VerusMcpServer {
    index: Arc<RwLock<Index>>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl VerusMcpServer {
    pub fn new(index: Index) -> Self {
        Self {
            index: Arc::new(RwLock::new(index)),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Search Verus proof/spec/exec functions by name substring. Returns matching function signatures with module paths and file locations.")]
    pub async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let kind = params.kind.as_deref().and_then(parse_kind);
        let results = idx.search(
            &params.query,
            kind,
            params.crate_name.as_deref(),
            params.module.as_deref(),
            params.trait_only,
        );

        if results.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No results for '{}'",
                params.query
            ))]));
        }

        let text: String = results
            .iter()
            .map(|e| e.format_full())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} results:\n\n{}",
            results.len(),
            text
        ))]))
    }

    #[tool(description = "Look up a Verus function by exact name. Returns full signature with requires/ensures clauses.")]
    pub async fn lookup(
        &self,
        Parameters(params): Parameters<LookupParams>,
    ) -> Result<CallToolResult, McpError> {
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let results = idx.lookup(&params.name);

        if results.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No function named '{}'",
                params.name
            ))]));
        }

        let text: String = results
            .iter()
            .map(|e| e.format_full())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Search within ensures clauses of Verus functions. Useful for finding lemmas that prove a specific property.")]
    pub async fn search_ensures(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let results = idx.search_ensures(&params.query);

        if results.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No ensures clauses matching '{}'",
                params.query
            ))]));
        }

        let text: String = results
            .iter()
            .map(|e| e.format_full())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} results:\n\n{}",
            results.len(),
            text
        ))]))
    }

    #[tool(description = "Search within requires clauses of Verus functions. Useful for finding what preconditions lemmas need.")]
    pub async fn search_requires(
        &self,
        Parameters(params): Parameters<ClauseSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let results = idx.search_requires(&params.query);

        if results.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No requires clauses matching '{}'",
                params.query
            ))]));
        }

        let text: String = results
            .iter()
            .map(|e| e.format_full())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} results:\n\n{}",
            results.len(),
            text
        ))]))
    }

    #[tool(description = "List all indexed modules with their item counts.")]
    pub async fn list_modules(&self) -> Result<CallToolResult, McpError> {
        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let modules = idx.list_modules();
        let total = idx.len();

        let mut text = format!("{} items across {} modules:\n\n", total, modules.len());
        for (path, count) in &modules {
            text.push_str(&format!("  {:4}  {}\n", count, path));
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Search Verus functions by parameter types, return type, or trait bounds. At least one of param_type, return_type, or type_bound is required. Examples: param_type='Vec2' finds orient2d/det2d; return_type='bool' finds predicates; type_bound='OrderedField' finds intersection functions; combine param_type='Point3' + name='orient' for orient3d family.")]
    pub async fn search_signature(
        &self,
        Parameters(params): Parameters<SignatureSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        if params.param_type.is_none() && params.return_type.is_none() && params.type_bound.is_none() {
            return Ok(CallToolResult::success(vec![Content::text(
                "At least one of param_type, return_type, or type_bound must be provided.",
            )]));
        }

        let idx = self.index.read().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;

        let kind = params.kind.as_deref().and_then(parse_kind);
        let results = idx.search_signature(
            params.param_type.as_deref(),
            params.return_type.as_deref(),
            params.type_bound.as_deref(),
            params.name.as_deref(),
            kind,
            params.crate_name.as_deref(),
            params.module.as_deref(),
        );

        if results.is_empty() {
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

        let text: String = results
            .iter()
            .map(|e| e.format_full())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} results:\n\n{}",
            results.len(),
            text
        ))]))
    }

    #[tool(description = "Rebuild the index from disk. Use after editing Verus source files. Only re-parses files that changed since the last index.")]
    pub async fn reindex(&self) -> Result<CallToolResult, McpError> {
        let old_cache = {
            let idx = self.index.read().map_err(|e| {
                McpError::internal_error(format!("Lock error: {}", e), None)
            })?;
            idx.file_cache().clone()
        };

        let (entries, new_cache) = indexer::build_index_incremental(&old_cache);
        let count = entries.len();
        let new_index = Index::new(entries, new_cache);

        let mut idx = self.index.write().map_err(|e| {
            McpError::internal_error(format!("Lock error: {}", e), None)
        })?;
        *idx = new_index;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Reindexed: {} items",
            count
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
