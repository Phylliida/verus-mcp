# verus-mcp

MCP server that indexes all Verus spec/proof/exec functions across the verus-cad workspace. Search by name, signature types, trait bounds, requires/ensures clauses — without grepping source or bloating context.

## Install

Build the binary:

```bash
cd verus-mcp
cargo build --release
```

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "verus": {
      "command": "/path/to/verus-cad/verus-mcp/target/release/verus-mcp",
      "cwd": "/path/to/verus-cad"
    }
  }
}
```

`cwd` must point to the verus-cad root — the indexer discovers sibling crates (verus-algebra, verus-linalg, verus-geometry, etc.) relative to the working directory.

Restart Claude Code after adding the config.

## Tools

| Tool | Description |
|------|-------------|
| `search` | Name substring search with optional kind/crate/module filters |
| `lookup` | Exact name match — returns full signature with requires/ensures |
| `search_ensures` | Find lemmas that prove a specific property |
| `search_requires` | Find what preconditions lemmas need |
| `search_signature` | Search by param types, return type, or trait bounds |
| `list_modules` | List all indexed modules with item counts |
| `reindex` | Rebuild index after editing source files |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VERUS_MCP_WORKSPACE` | Override workspace root detection |
| `VERUS_MCP_ROOTS` | Override crate roots (format: `crate=path,crate=path,...`) |



I use this with this CLAUDE.md

```
# verus-cad

## General rules:
- Never rederive something just because it's private, just make it public, you have permission to edit any verus-* repo
- Feel free to add a proof/lemma to any verus-* repo if it doesn't exist and you need it
- Remember if resource limit esceeded it's best to expand stuff out until it's not (just help z3 along)
- Always call context_list first, then resume context with replay=True when starting fresh/after a compaction, this helps you get up to speed quicker and avoids redoing search work.

## MCP: Verus Proof Index

This project has a Verus MCP server (`verus-mcp`) that indexes all spec/proof/exec functions, types, traits, and impls across the codebase. Prefer these tools when searching for Verus items:

### Function Search
- `search(query, details?)` — Browse functions by name substring. Ranked: exact > prefix > substring. Includes fuzzy fallback when few results found (capped at 4 with results, 10 without). Set `details=true` for full signatures with requires/ensures.
- `search_ensures(query)` — Find lemmas that prove a specific property. Clause snippets centered around match.
- `search_requires(query)` — Find what preconditions a lemma needs.
- `search_signature(param_type, return_type, type_bound)` — Find functions by type signature.
- `search_body(query)` — Find functions that call a specific lemma or use a pattern in their body.
- `search_doc(query)` — Search within doc comments of functions and types.
- `lookup(name)` — Get full details (signature, requires/ensures, file:line-endline, module) for a single function or type.
- `lookup_source(name)` — Get full source code of a function (reads from disk using indexed line range).
- `batch_lookup(names)` — Look up multiple functions/types by exact name in one call (max 10). Returns full signatures.

### Type & Trait Search
- `search_types(query)` — Browse structs, enums, and type aliases by name substring.
- `search_trait(name)` — Show trait definition + all implementors.
- `browse_module(path)` — List all functions and types in a module or crate. Supports crate-qualified paths (e.g., `verus_topology`, `crate::verus_topology`, `verus_topology::mesh`).

### Dependency Tracking
- `find_dependencies(name, direction?)` — Call graph: "callers" (default) or "callees".

### Verification
- `check(crate_name, module?)` — Run Verus verification. Without `module`: verifies entire crate. With `module`: verifies only that module (much faster for iteration). Accepts file paths (`src/runtime/polygon.rs`) or module paths (`runtime::polygon`). Returns clean summary on success, extracted error diagnostics on failure. 10-minute timeout.
- `profile(crate_name, module?, top_n?)` — Per-function SMT time and rlimit breakdown. Sorted table of hottest functions + per-module summary. Use rlimit (deterministic) not SMT time (2x variance) to measure optimization impact. Default top 25.

### Context Management
- `context_list()` — **Must be called first** before `context_activate`. Lists recent contexts with item counts and last-used times. This ensures you see existing contexts before creating a new one.
- `context_activate(name)` — Activate a context. Requires `context_list` to have been called first.
  - **Existing name**: Loads context and replays all captured signatures.
  - **New name**: Creates empty context and activates it (pick a name that 4-5 words and fairly descriptive and specific).
- Items are auto-captured on `lookup`, `lookup_source`, and `batch_lookup`.
- Persisted to `~/.verus-mcp/contexts/<name>.json`. Signatures resolved live from index on replay.
- Name contexts after the task (e.g., `triangle-intersection`, `orient3d-proofs`, `construction-phases`).
- **After context compaction**: Call `context_list()` then `context_activate(name)` to restore all previously looked-up signatures into the new context window. It's recommended to resume an existing context instead of creating a new one.

### Utilities
- `list_modules()` — See all indexed modules grouped by crate.
- `stats()` — Show index statistics: counts by kind (spec/proof/exec), by crate, and assume(false) proof debt.
- `reindex()` — Force rebuild index. **Not normally needed** — the server auto-reindexes when `.rs` files change (500ms debounce).

**Workflow:** Use `search` / `search_ensures` / `search_requires` to browse, then `lookup` or `batch_lookup` to drill into specific functions. Use `search(query, details=true)` when you want full details inline without a separate lookup call.

Crate roots are auto-discovered — any `verus-*/src` directory in the workspace is indexed automatically.

All search tools accept optional `limit` (default 50) and `offset` (default 0) parameters for pagination.

`search_ensures`, `search_requires`, `search_body`, and `search_types` also accept optional `crate_name` and `module` filters.

`search_ensures`, `search_requires`, `search_body`, and `search_doc` queries support regex (e.g., `div.*mul.*eqv`). If the query isn't valid regex, it falls back to plain substring matching. All regex is case-insensitive.

`search_ensures`, `search_requires`, `search_body`, and `search_doc` also accept optional `name` filter to combine name + clause/body/doc search (e.g., find functions named "*cancel*" whose ensures mentions "eqv").
```

ymmv
