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
