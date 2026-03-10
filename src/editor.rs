use crate::types::FnKind;

// ---------------------------------------------------------------------------
// Located‐item structs — carry byte offsets for splicing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LocatedFn {
    pub name: String,
    /// "Type::method" for impl methods
    pub qualified_name: String,
    pub kind: Option<FnKind>,
    /// Everything before the body `{`
    pub signature: String,
    pub start_byte: usize,
    pub end_byte: usize,
    /// For methods, the impl target type
    pub impl_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LocatedType {
    pub name: String,
    /// "struct", "enum", or "type"
    pub kind: String,
    pub signature: String,
    pub start_byte: usize,
    pub end_byte: usize,
}

#[derive(Debug, Clone)]
pub struct LocatedImpl {
    pub type_name: String,
    pub trait_name: Option<String>,
    pub signature: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub methods: Vec<LocatedFn>,
}

#[derive(Debug, Clone)]
pub struct LocatedTrait {
    pub name: String,
    pub signature: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub methods: Vec<LocatedFn>,
}

#[derive(Debug, Clone)]
pub struct LocatedUse {
    pub full_text: String,
    pub path: String,
    pub start_byte: usize,
    pub end_byte: usize,
}

#[derive(Debug, Clone)]
pub struct LocatedVerusBlock {
    pub start_byte: usize,
    pub end_byte: usize,
    /// Start of the inner body (after the opening `{` + whitespace)
    pub body_start_byte: usize,
    /// End of the inner body (before the closing `}`)
    pub body_end_byte: usize,
}

#[derive(Debug, Clone, Default)]
pub struct FileItems {
    pub functions: Vec<LocatedFn>,
    pub types: Vec<LocatedType>,
    pub impls: Vec<LocatedImpl>,
    pub traits: Vec<LocatedTrait>,
    pub uses: Vec<LocatedUse>,
    pub verus_blocks: Vec<LocatedVerusBlock>,
}

// ---------------------------------------------------------------------------
// Tree‑sitter helpers
// ---------------------------------------------------------------------------

fn node_text<'a>(node: &tree_sitter::Node, source: &'a str) -> &'a str {
    node.utf8_text(source.as_bytes()).unwrap_or("")
}

fn extract_fn_kind(node: &tree_sitter::Node, source: &str) -> Option<FnKind> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "function_modifiers" {
            let mut mc = child.walk();
            for modifier in child.children(&mut mc) {
                match node_text(&modifier, source) {
                    "spec" => return Some(FnKind::Spec),
                    "proof" => return Some(FnKind::Proof),
                    "exec" => return Some(FnKind::Exec),
                    _ => {}
                }
            }
        }
    }
    None
}

/// Build signature text: everything from start of the node up to (but not
/// including) the body block `{`. Falls back to full node text for
/// signature-only items.
fn extract_signature(node: &tree_sitter::Node, source: &str) -> String {
    if let Some(body) = node.child_by_field_name("body") {
        let sig_end = body.start_byte();
        source[node.start_byte()..sig_end].trim_end().to_string()
    } else {
        node_text(node, source).to_string()
    }
}

// ---------------------------------------------------------------------------
// Core parsing
// ---------------------------------------------------------------------------

pub fn parse_file(source: &str) -> Result<FileItems, String> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_verus::LANGUAGE.into())
        .map_err(|e| format!("Failed to load Verus grammar: {}", e))?;

    let tree = parser
        .parse(source.as_bytes(), None)
        .ok_or_else(|| "Failed to parse source".to_string())?;

    let root = tree.root_node();
    let mut items = FileItems::default();

    collect_items(&root, source, None, &mut items);

    Ok(items)
}

fn collect_items(
    node: &tree_sitter::Node,
    source: &str,
    impl_type: Option<&str>,
    items: &mut FileItems,
) {
    // Handle ERROR nodes: try to extract orphaned functions
    if node.kind() == "ERROR" {
        extract_orphaned_functions(node, source, impl_type, items);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "use_declaration" => {
                let full_text = node_text(&child, source).to_string();
                // Extract path: strip "use " prefix and trailing ";"
                let path = full_text
                    .strip_prefix("use ")
                    .unwrap_or(&full_text)
                    .trim_end_matches(';')
                    .trim()
                    .to_string();
                items.uses.push(LocatedUse {
                    full_text,
                    path,
                    start_byte: child.start_byte(),
                    end_byte: child.end_byte(),
                });
            }
            "verus_block" => {
                let vb_start = child.start_byte();
                let vb_end = child.end_byte();

                if let Some(body) = child.child_by_field_name("body") {
                    // body is typically a declaration_list
                    let body_start = body.start_byte();
                    let body_end = body.end_byte();

                    // The body_start_byte should be after the opening `{` of the
                    // declaration_list. We use the first byte inside the list.
                    // declaration_list looks like `{ ... }` so start_byte is `{`.
                    // We want the byte right after `{`.
                    let inner_start = if body_start < source.len()
                        && source.as_bytes()[body_start] == b'{'
                    {
                        body_start + 1
                    } else {
                        body_start
                    };
                    let inner_end = if body_end > 0
                        && source.as_bytes()[body_end - 1] == b'}'
                    {
                        body_end - 1
                    } else {
                        body_end
                    };

                    items.verus_blocks.push(LocatedVerusBlock {
                        start_byte: vb_start,
                        end_byte: vb_end,
                        body_start_byte: inner_start,
                        body_end_byte: inner_end,
                    });

                    // Recurse into the body
                    collect_items(&body, source, impl_type, items);
                } else {
                    // verus block without body field — record block, recurse children
                    items.verus_blocks.push(LocatedVerusBlock {
                        start_byte: vb_start,
                        end_byte: vb_end,
                        body_start_byte: vb_start,
                        body_end_byte: vb_end,
                    });
                    collect_items(&child, source, impl_type, items);
                }
            }
            "declaration_list" => {
                collect_items(&child, source, impl_type, items);
            }
            "function_item" | "function_signature_item" => {
                if let Some(f) = extract_located_fn(&child, source, impl_type) {
                    items.functions.push(f);
                }
            }
            "impl_item" => {
                collect_impl(&child, source, items);
            }
            "trait_item" => {
                collect_trait(&child, source, items);
            }
            "struct_item" | "enum_item" | "type_item" => {
                if let Some(t) = extract_located_type(&child, source) {
                    items.types.push(t);
                }
            }
            "ERROR" | "block" | "expression_statement" => {
                collect_items(&child, source, impl_type, items);
                if child.kind() == "ERROR" {
                    extract_orphaned_functions(&child, source, impl_type, items);
                }
            }
            _ => {}
        }
    }
}

fn extract_located_fn(
    node: &tree_sitter::Node,
    source: &str,
    impl_type: Option<&str>,
) -> Option<LocatedFn> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(&name_node, source).to_string();
    let kind = extract_fn_kind(node, source);
    let signature = extract_signature(node, source);
    let qualified_name = match impl_type {
        Some(t) => format!("{}::{}", t, name),
        None => name.clone(),
    };

    Some(LocatedFn {
        name,
        qualified_name,
        kind,
        signature,
        start_byte: node.start_byte(),
        end_byte: node.end_byte(),
        impl_type: impl_type.map(|s| s.to_string()),
    })
}

fn extract_located_type(node: &tree_sitter::Node, source: &str) -> Option<LocatedType> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(&name_node, source).to_string();
    let kind = match node.kind() {
        "struct_item" => "struct",
        "enum_item" => "enum",
        "type_item" => "type",
        _ => return None,
    };
    let signature = extract_signature(node, source);

    Some(LocatedType {
        name,
        kind: kind.to_string(),
        signature,
        start_byte: node.start_byte(),
        end_byte: node.end_byte(),
    })
}

fn collect_impl(
    impl_node: &tree_sitter::Node,
    source: &str,
    items: &mut FileItems,
) {
    let type_name = impl_node
        .child_by_field_name("type")
        .map(|n| node_text(&n, source).to_string())
        .unwrap_or_default();

    let trait_name = impl_node
        .child_by_field_name("trait")
        .map(|n| node_text(&n, source).to_string());

    let signature = extract_signature(impl_node, source);

    let mut methods = Vec::new();
    if let Some(body) = impl_node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "function_item" || child.kind() == "function_signature_item" {
                if let Some(f) = extract_located_fn(&child, source, Some(&type_name)) {
                    methods.push(f.clone());
                    items.functions.push(f);
                }
            }
        }
    }

    items.impls.push(LocatedImpl {
        type_name,
        trait_name,
        signature,
        start_byte: impl_node.start_byte(),
        end_byte: impl_node.end_byte(),
        methods,
    });
}

fn collect_trait(
    trait_node: &tree_sitter::Node,
    source: &str,
    items: &mut FileItems,
) {
    let name = trait_node
        .child_by_field_name("name")
        .map(|n| node_text(&n, source).to_string())
        .unwrap_or_default();

    let signature = extract_signature(trait_node, source);

    let mut methods = Vec::new();
    if let Some(body) = trait_node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "function_item" || child.kind() == "function_signature_item" {
                if let Some(f) = extract_located_fn(&child, source, None) {
                    methods.push(f.clone());
                    items.functions.push(f);
                }
            }
        }
    }

    items.traits.push(LocatedTrait {
        name,
        signature,
        start_byte: trait_node.start_byte(),
        end_byte: trait_node.end_byte(),
        methods,
    });
}

/// Extract orphaned function signatures from ERROR nodes (same pattern as parser.rs).
fn extract_orphaned_functions(
    error_node: &tree_sitter::Node,
    source: &str,
    impl_type: Option<&str>,
    items: &mut FileItems,
) {
    let child_count = error_node.child_count();
    let existing: std::collections::HashSet<String> =
        items.functions.iter().map(|f| f.name.clone()).collect();

    let mut i = 0;
    while i < child_count {
        let child = match error_node.child(i) {
            Some(c) => c,
            None => { i += 1; continue; }
        };

        if child.kind() != "function_modifiers" {
            i += 1;
            continue;
        }

        let mods_node = child;
        let mods_idx = i;

        // Extract kind
        let mut kind = None;
        {
            let mut mc = mods_node.walk();
            for modifier in mods_node.children(&mut mc) {
                match node_text(&modifier, source) {
                    "spec" => kind = Some(FnKind::Spec),
                    "proof" => kind = Some(FnKind::Proof),
                    "exec" => kind = Some(FnKind::Exec),
                    _ => {}
                }
            }
        }

        // Scan for identifier (name)
        let mut j = mods_idx + 1;
        let mut name_text: Option<String> = None;
        while j < child_count {
            if let Some(n) = error_node.child(j) {
                if n.kind() == "identifier" {
                    name_text = Some(node_text(&n, source).to_string());
                    j += 1;
                    break;
                }
                if n.kind() == "function_modifiers" || n.kind() == "function_item" {
                    break;
                }
                if !n.is_named() || n.kind() == "visibility_modifier" {
                    j += 1;
                    continue;
                }
                break;
            } else {
                break;
            }
        }

        let name_text = match name_text {
            Some(n) => n,
            None => { i = j.max(mods_idx + 1); continue; }
        };

        if existing.contains(&name_text) {
            i = j;
            continue;
        }

        // Determine start: include visibility_modifier if present
        let start_byte = if mods_idx > 0 {
            error_node.child(mods_idx - 1)
                .filter(|n| n.kind() == "visibility_modifier")
                .map(|n| n.start_byte())
                .unwrap_or_else(|| mods_node.start_byte())
        } else {
            mods_node.start_byte()
        };

        let mut end_byte = mods_node.end_byte();

        // Collect end_byte by scanning forward
        while j < child_count {
            let n = match error_node.child(j) {
                Some(n) => n,
                None => break,
            };
            match n.kind() {
                "visibility_modifier" | "function_modifiers" | "function_item" => break,
                _ => {
                    end_byte = n.end_byte();
                }
            }
            j += 1;
        }

        // Build signature from source text up to the body block
        let fn_text = &source[start_byte..end_byte];
        let sig = if let Some(brace_pos) = fn_text.find('{') {
            fn_text[..brace_pos].trim_end().to_string()
        } else {
            fn_text.trim_end().to_string()
        };

        let qualified_name = match impl_type {
            Some(t) => format!("{}::{}", t, name_text),
            None => name_text.clone(),
        };

        items.functions.push(LocatedFn {
            name: name_text,
            qualified_name,
            kind,
            signature: sig,
            start_byte,
            end_byte,
            impl_type: impl_type.map(|s| s.to_string()),
        });

        i = j;
    }
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

/// List all items in a file, optionally filtered by kind.
/// Returns a formatted string with one signature per line.
pub fn list_items(source: &str, kind_filter: Option<&str>) -> Result<String, String> {
    let items = parse_file(source)?;

    // Helper: check if a byte offset falls inside any verus block
    let in_verus = |byte: usize| -> bool {
        items.verus_blocks.iter().any(|vb| byte >= vb.body_start_byte && byte < vb.body_end_byte)
    };

    // Collect (start_byte, formatted, is_in_verus) entries
    let mut entries: Vec<(usize, String, bool)> = Vec::new();

    // Collect impl method byte ranges to skip in top-level function list
    let impl_fn_bytes: std::collections::HashSet<usize> = items
        .impls
        .iter()
        .flat_map(|im| im.methods.iter().map(|m| m.start_byte))
        .collect();

    // Functions (optionally filtered), excluding impl methods (shown under their impl)
    for f in &items.functions {
        if impl_fn_bytes.contains(&f.start_byte) {
            continue;
        }
        let include = match kind_filter {
            None => true,
            Some("fn") => f.kind.is_none(),
            Some("spec") => f.kind == Some(FnKind::Spec),
            Some("proof") => f.kind == Some(FnKind::Proof),
            Some("exec") => f.kind == Some(FnKind::Exec),
            Some("struct") | Some("enum") | Some("trait") | Some("impl") => false,
            Some(_) => true,
        };
        if include {
            // Show signature with body placeholder: `fn foo() { ... }`
            let has_body = f.end_byte > f.start_byte
                && source[f.start_byte..f.end_byte].trim_end().ends_with('}');
            let text = if has_body {
                format!("{} {{ ... }}", f.signature)
            } else {
                // No body (e.g. trait method signature) — show as-is
                f.signature.clone()
            };
            entries.push((f.start_byte, text, in_verus(f.start_byte)));
        }
    }

    // Types
    let show_types = match kind_filter {
        None => true,
        Some("struct") | Some("enum") | Some("type") => true,
        _ => false,
    };
    if show_types {
        for t in &items.types {
            let include = match kind_filter {
                None => true,
                Some(k) => t.kind == k,
            };
            if include {
                let has_body = t.kind != "type"; // type aliases have no body
                let text = if has_body {
                    format!("{} {{ ... }}", t.signature)
                } else {
                    t.signature.clone()
                };
                entries.push((t.start_byte, text, in_verus(t.start_byte)));
            }
        }
    }

    // Traits — show header, then each method with signature on its own line
    if kind_filter.is_none() || kind_filter == Some("trait") {
        for t in &items.traits {
            if t.methods.is_empty() {
                entries.push((t.start_byte, format!("{} {{ ... }}", t.signature), in_verus(t.start_byte)));
            } else {
                let mut lines = vec![format!("{} {{", t.signature)];
                for m in &t.methods {
                    let has_body = source[m.start_byte..m.end_byte].trim_end().ends_with('}');
                    if has_body {
                        lines.push(format!("    {} {{ ... }}", m.signature));
                    } else {
                        lines.push(format!("    {}", m.signature));
                    }
                }
                lines.push("}".to_string());
                entries.push((t.start_byte, lines.join("\n"), in_verus(t.start_byte)));
            }
        }
    }

    // Impls — show header, then each method with signature on its own line
    if kind_filter.is_none() || kind_filter == Some("impl") {
        for im in &items.impls {
            if im.methods.is_empty() {
                entries.push((im.start_byte, format!("{} {{ ... }}", im.signature), in_verus(im.start_byte)));
            } else {
                let mut lines = vec![format!("{} {{", im.signature)];
                for m in &im.methods {
                    let has_body = source[m.start_byte..m.end_byte].trim_end().ends_with('}');
                    if has_body {
                        lines.push(format!("    {} {{ ... }}", m.signature));
                    } else {
                        lines.push(format!("    {}", m.signature));
                    }
                }
                lines.push("}".to_string());
                entries.push((im.start_byte, lines.join("\n"), in_verus(im.start_byte)));
            }
        }
    }

    if entries.is_empty() {
        return Ok("No items found.".to_string());
    }

    // Sort by source position
    entries.sort_by_key(|(byte, _, _)| *byte);

    // Group items: wrap consecutive verus items in verus! { ... }
    let mut output = Vec::new();
    let mut in_verus_group = false;

    for (_byte, text, is_verus) in &entries {
        if *is_verus && !in_verus_group {
            output.push("verus! {".to_string());
            in_verus_group = true;
        } else if !is_verus && in_verus_group {
            output.push("}".to_string());
            output.push(String::new());
            in_verus_group = false;
        }

        if in_verus_group {
            // Indent every line of multi-line entries (e.g. impl blocks)
            for line in text.lines() {
                output.push(format!("    {}", line));
            }
        } else {
            output.push(text.clone());
        }
    }

    if in_verus_group {
        output.push("}".to_string());
    }

    let result = output.join("\n");
    if result.contains("{ ... }") {
        Ok(format!("{}\n\n// {{ ... }} = body hidden. Use `read` with `name` to view a specific function.", result))
    } else {
        Ok(result)
    }
}

/// Format a trait/impl block with method stubs (signatures + `...` for bodies).
fn format_block_summary(source: &str, signature: &str, methods: &[LocatedFn]) -> String {
    let mut lines = vec![format!("{} {{", signature)];
    for m in methods {
        let has_body = source[m.start_byte..m.end_byte].trim_end().ends_with('}');
        if has_body {
            lines.push(format!("    {} {{ ... }}", m.signature));
        } else {
            lines.push(format!("    {}", m.signature));
        }
    }
    lines.push("}".to_string());
    if methods.iter().any(|m| source[m.start_byte..m.end_byte].trim_end().ends_with('}')) {
        lines.push(String::new());
        lines.push("// { ... } = body hidden. Use `read` with method name to view full source.".to_string());
    }
    lines.join("\n")
}

/// Return the source text of a named item (function, trait, impl, type).
/// Searches functions first, then traits, impls, and types.
/// Supports qualified names like "Type::method".
pub fn read_fn(source: &str, name: &str) -> Result<String, String> {
    let items = parse_file(source)?;

    // Try functions first
    if let Ok(found) = find_fn(&items, name) {
        return Ok(source[found.start_byte..found.end_byte].to_string());
    }

    let name_stripped = strip_generics(name);

    // Try traits — show signature + method stubs
    for t in &items.traits {
        if t.name == name || strip_generics(&t.name) == name_stripped {
            return Ok(format_block_summary(source, &t.signature, &t.methods));
        }
    }

    // Try impls — show signature + method stubs
    for im in &items.impls {
        let label = if let Some(ref tr) = im.trait_name {
            format!("{} for {}", tr, im.type_name)
        } else {
            im.type_name.clone()
        };
        if im.type_name == name
            || label == name
            || strip_generics(&im.type_name) == name_stripped
            || strip_generics(&label) == name_stripped
        {
            return Ok(format_block_summary(source, &im.signature, &im.methods));
        }
    }

    // Try types (struct, enum, type alias)
    for ty in &items.types {
        if ty.name == name || strip_generics(&ty.name) == name_stripped {
            return Ok(source[ty.start_byte..ty.end_byte].to_string());
        }
    }

    // Nothing found — build combined list
    let mut available: Vec<&str> = items.functions.iter().map(|f| f.qualified_name.as_str()).collect();
    available.extend(items.traits.iter().map(|t| t.name.as_str()));
    available.extend(items.impls.iter().map(|im| im.type_name.as_str()));
    available.extend(items.types.iter().map(|ty| ty.name.as_str()));
    Err(format!(
        "Item '{}' not found. Available: {}",
        name,
        if available.is_empty() { "(none)".to_string() } else { available.join(", ") }
    ))
}

/// Find which function(s) contain a given substring. Returns qualified names.
pub fn find_containing_fn(source: &str, needle: &str) -> Result<Vec<String>, String> {
    let items = parse_file(source)?;
    let matches: Vec<String> = items
        .functions
        .iter()
        .filter(|f| source[f.start_byte..f.end_byte].contains(needle))
        .map(|f| f.qualified_name.clone())
        .collect();
    Ok(matches)
}

/// Levenshtein edit distance between two strings.
fn edit_distance(a: &str, b: &str) -> usize {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let m = a_bytes.len();
    let n = b_bytes.len();
    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];
    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_bytes[i - 1] == b_bytes[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

/// Per-line edit distance: sum of edit distances between corresponding lines,
/// plus a large penalty per added/removed line. Returns None if over max_total.
fn line_level_distance(needle_lines: &[&str], candidate_lines: &[&str], max_total: usize) -> Option<usize> {
    if needle_lines.len() != candidate_lines.len() {
        return None; // line count must match — we don't fuzzy across line boundaries
    }
    let mut total = 0usize;
    let max_per_line = 10;
    for (n, c) in needle_lines.iter().zip(candidate_lines.iter()) {
        let d = edit_distance(n.trim(), c.trim());
        if d > max_per_line {
            return None;
        }
        total += d;
        if total > max_total {
            return None;
        }
    }
    Some(total)
}

/// Try to fuzzy-match `needle` (multi-line) against sliding windows in `haystack`.
/// Returns (byte_offset, matched_length, distance) of unique best match, or None.
fn fuzzy_find(haystack: &str, needle: &str, max_total: usize) -> Option<(usize, usize, usize)> {
    let needle_lines: Vec<&str> = needle.lines().collect();
    let n_lines = needle_lines.len();
    if n_lines == 0 {
        return None;
    }

    let hay_lines: Vec<&str> = haystack.lines().collect();
    if hay_lines.len() < n_lines {
        return None;
    }

    // Precompute byte offset of each line in haystack
    let mut line_byte_offsets = Vec::with_capacity(hay_lines.len());
    let mut offset = 0usize;
    for line in &hay_lines {
        line_byte_offsets.push(offset);
        offset += line.len() + 1; // +1 for newline
    }

    let mut best: Option<(usize, usize, usize)> = None; // (line_idx, dist, count_at_dist)
    let mut best_count = 0usize;

    for start in 0..=(hay_lines.len() - n_lines) {
        let window = &hay_lines[start..start + n_lines];
        if let Some(dist) = line_level_distance(&needle_lines, window, max_total) {
            match &best {
                None => {
                    best = Some((start, dist, 1));
                    best_count = 1;
                }
                Some((_, best_dist, _)) => {
                    if dist < *best_dist {
                        best = Some((start, dist, 1));
                        best_count = 1;
                    } else if dist == *best_dist {
                        best_count += 1;
                    }
                }
            }
        }
    }

    match best {
        Some((line_idx, dist, _)) if best_count == 1 && dist > 0 => {
            let byte_start = line_byte_offsets[line_idx];
            let end_line = line_idx + n_lines - 1;
            let byte_end = line_byte_offsets[end_line] + hay_lines[end_line].len();
            // Include trailing newline if present
            let byte_end = if byte_end < haystack.len() && haystack.as_bytes()[byte_end] == b'\n' {
                byte_end
            } else {
                byte_end
            };
            Some((byte_start, byte_end - byte_start, dist))
        }
        _ => None,
    }
}

/// Wildcard gap type in an ellipsis pattern.
#[derive(Debug, Clone, PartialEq)]
enum EllipsisGap {
    /// `...` — match smallest arbitrary text
    Any,
    /// `{ ... }` on its own line — match a brace-balanced block (from `{` to matching `}`)
    BraceBlock,
    /// `{ ... }` at end of a line (inline) — literal prefix + brace-balanced block
    InlineBraceBlock,
}

/// A segment is either a literal string or a gap (wildcard).
#[derive(Debug, Clone)]
enum EllipsisPart {
    Literal(String),
    Gap(EllipsisGap),
}

/// Split old_string on ellipsis wildcards.
/// Recognized patterns:
/// - A line whose trimmed content is `...` → Gap(Any)
/// - A line whose trimmed content is `{ ... }` or `{...}` → Gap(BraceBlock)
/// - A line ending with `{ ... }` or `{...}` (inline) → literal prefix + Gap(BraceBlock)
/// Returns None if no wildcards are found.
fn split_on_ellipsis(old_string: &str) -> Option<Vec<EllipsisPart>> {
    let lines: Vec<&str> = old_string.lines().collect();
    let has_wildcard = lines.iter().any(|l| {
        let t = l.trim();
        t == "..." || t == "{ ... }" || t == "{...}"
            || t.ends_with("{ ... }") || t.ends_with("{...}")
    });
    if !has_wildcard {
        return None;
    }
    let mut parts = Vec::new();
    let mut current: Vec<&str> = Vec::new();

    for line in &lines {
        let t = line.trim();
        if t == "..." {
            let text = current.join("\n");
            if !text.is_empty() {
                parts.push(EllipsisPart::Literal(text));
            }
            current.clear();
            parts.push(EllipsisPart::Gap(EllipsisGap::Any));
        } else if t == "{ ... }" || t == "{...}" {
            let text = current.join("\n");
            if !text.is_empty() {
                parts.push(EllipsisPart::Literal(text));
            }
            current.clear();
            parts.push(EllipsisPart::Gap(EllipsisGap::BraceBlock));
        } else if t.ends_with("{ ... }") || t.ends_with("{...}") {
            // Inline trailing brace block: split line at the `{`
            let suffix = if t.ends_with("{ ... }") { "{ ... }" } else { "{...}" };
            let prefix = &line[..line.len() - suffix.len()];
            // Add prefix (with preceding lines) as literal
            current.push(prefix);
            let text = current.join("\n");
            if !text.is_empty() {
                parts.push(EllipsisPart::Literal(text));
            }
            current.clear();
            parts.push(EllipsisPart::Gap(EllipsisGap::InlineBraceBlock));
        } else {
            current.push(line);
        }
    }
    let text = current.join("\n");
    if !text.is_empty() {
        parts.push(EllipsisPart::Literal(text));
    }
    Some(parts)
}

/// Find the byte offset right after the matching `}` for a `{` at `start` in `text`.
/// `start` should point to a position where we search for the first `{`.
fn find_matching_brace(text: &str, start: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut found_open = false;
    for (i, ch) in text[start..].char_indices() {
        match ch {
            '{' => { depth += 1; found_open = true; }
            '}' => {
                depth -= 1;
                if found_open && depth == 0 {
                    return Some(start + i + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

/// Find a literal segment (possibly multi-line) in haystack starting from `from_byte`,
/// ignoring leading/trailing whitespace per line.
/// If `prefix_last_line` is true, the last line matches as a prefix (for inline `{ ... }`).
/// Returns (start_byte, end_byte) of first match.
fn find_literal_normalized(
    haystack: &str,
    literal: &str,
    from_byte: usize,
    prefix_last_line: bool,
) -> Option<(usize, usize)> {
    let lit_lines: Vec<&str> = literal.lines().collect();
    if lit_lines.is_empty() {
        return Some((from_byte, from_byte));
    }

    let lit_trimmed: Vec<&str> = lit_lines.iter().map(|l| l.trim()).collect();
    if lit_trimmed.iter().all(|l| l.is_empty()) {
        return None;
    }

    // Build line index for haystack: (byte_offset, line_text)
    let mut hay_lines: Vec<(usize, &str)> = Vec::new();
    let mut offset = 0;
    for line in haystack.lines() {
        hay_lines.push((offset, line));
        offset += line.len() + 1;
    }

    let n = lit_trimmed.len();

    for start_idx in 0..hay_lines.len() {
        let (line_start, _) = hay_lines[start_idx];
        if line_start < from_byte { continue; }
        if start_idx + n > hay_lines.len() { break; }

        let matches = (0..n).all(|j| {
            let hay_t = hay_lines[start_idx + j].1.trim();
            let lit_t = lit_trimmed[j];
            if j == n - 1 && prefix_last_line {
                hay_t.starts_with(lit_t)
            } else {
                hay_t == lit_t
            }
        });

        if matches {
            let start_byte = hay_lines[start_idx].0;
            let end_idx = start_idx + n - 1;
            let end_byte = if prefix_last_line {
                // For prefix match: end at the end of the matched prefix content in the line
                // Find where the trimmed literal prefix ends in the actual haystack line
                let (line_off, line_text) = hay_lines[end_idx];
                let leading = line_text.len() - line_text.trim_start().len();
                line_off + leading + lit_trimmed[n - 1].len()
            } else {
                let (line_off, line_text) = hay_lines[end_idx];
                line_off + line_text.len()
            };
            return Some((start_byte, end_byte));
        }
    }
    None
}

/// Find a literal in haystack ignoring indent, returning all matches (for ambiguity check).
fn find_all_literal_normalized(
    haystack: &str,
    literal: &str,
) -> Vec<(usize, usize)> {
    let mut results = Vec::new();
    let mut from = 0;
    while let Some((start, end)) = find_literal_normalized(haystack, literal, from, false) {
        results.push((start, end));
        // Move past the start of this match to find next
        // Advance by at least one line
        from = haystack[start..].find('\n').map(|p| start + p + 1).unwrap_or(haystack.len());
    }
    results
}

/// Adjust new_string indentation to match the indentation at match_start in source.
fn adjust_new_indent(source: &str, match_start: usize, old_string: &str, new_string: &str) -> String {
    // Find indent of source at match_start
    let line_start = source[..match_start].rfind('\n').map(|p| p + 1).unwrap_or(0);
    let src_indent = leading_whitespace(&source[line_start..]);

    // Find indent of old_string's first non-empty line
    let old_indent = old_string.lines()
        .find(|l| !l.trim().is_empty())
        .map(leading_whitespace)
        .unwrap_or("");

    if src_indent == old_indent {
        return new_string.to_string();
    }

    new_string.lines().map(|line| {
        if line.trim().is_empty() {
            line.to_string()
        } else {
            let li = leading_whitespace(line);
            if li.len() >= old_indent.len() && li.starts_with(old_indent) {
                let extra = &li[old_indent.len()..];
                format!("{}{}{}", src_indent, extra, line.trim_start())
            } else {
                format!("{}{}", src_indent, line.trim_start())
            }
        }
    }).collect::<Vec<_>>().join("\n")
}

/// Find all (start, end) byte spans in `haystack` that match the ellipsis pattern.
/// Matching ignores leading/trailing whitespace per line.
/// `...` gaps match the smallest text; `{ ... }` gaps match a brace-balanced block.
fn find_with_ellipsis(haystack: &str, parts: &[EllipsisPart]) -> Vec<(usize, usize)> {
    // Collect literal segments with their part index
    let literals: Vec<(usize, &str)> = parts.iter().enumerate().filter_map(|(i, p)| {
        if let EllipsisPart::Literal(s) = p {
            if !s.is_empty() { return Some((i, s.as_str())); }
        }
        None
    }).collect();

    if literals.is_empty() {
        return vec![];
    }

    let (first_idx, first_lit) = literals[0];
    // Check if the part after the first literal is an InlineBraceBlock
    let first_prefix = first_idx + 1 < parts.len()
        && matches!(&parts[first_idx + 1], EllipsisPart::Gap(EllipsisGap::InlineBraceBlock));
    let mut results = Vec::new();
    let mut search_from = 0;

    while let Some((match_start, seg_end)) = find_literal_normalized(haystack, first_lit, search_from, first_prefix) {
        let mut current_end = seg_end;
        let mut valid = true;

        for &(part_idx, lit) in &literals[1..] {
            // Check if the gap before this literal is a brace block
            let gap_is_brace = part_idx > 0 && matches!(
                &parts[part_idx - 1],
                EllipsisPart::Gap(EllipsisGap::BraceBlock) | EllipsisPart::Gap(EllipsisGap::InlineBraceBlock)
            );
            // Check if the part after this literal is an InlineBraceBlock
            let this_prefix = part_idx + 1 < parts.len()
                && matches!(&parts[part_idx + 1], EllipsisPart::Gap(EllipsisGap::InlineBraceBlock));

            if gap_is_brace {
                if let Some(after_brace) = find_matching_brace(haystack, current_end) {
                    if let Some((_seg_start, seg_end)) = find_literal_normalized(haystack, lit, after_brace, this_prefix) {
                        current_end = seg_end;
                    } else {
                        valid = false;
                        break;
                    }
                } else {
                    valid = false;
                    break;
                }
            } else {
                // Regular `...` gap — find nearest occurrence
                if let Some((_seg_start, seg_end)) = find_literal_normalized(haystack, lit, current_end, this_prefix) {
                    current_end = seg_end;
                } else {
                    valid = false;
                    break;
                }
            }
        }

        // If the last part is a brace block gap, consume it
        if valid {
            if let Some(EllipsisPart::Gap(EllipsisGap::BraceBlock | EllipsisGap::InlineBraceBlock)) = parts.last() {
                if let Some(after_brace) = find_matching_brace(haystack, current_end) {
                    current_end = after_brace;
                } else {
                    valid = false;
                }
            }
        }

        if valid {
            results.push((match_start, current_end));
        }
        // Advance search past this match start
        search_from = haystack[match_start..].find('\n').map(|p| match_start + p + 1).unwrap_or(haystack.len());
    }

    results
}

/// Get the leading whitespace of a line.
fn leading_whitespace(line: &str) -> &str {
    let trimmed = line.trim_start();
    &line[..line.len() - trimmed.len()]
}

/// File-level edit: find `old_string` anywhere in the source and replace it.
/// Matching ignores leading/trailing whitespace per line (indent-insensitive).
/// Supports ellipsis wildcards (`...`, `{ ... }`) and fuzzy matching.
pub fn edit_file(
    source: &str,
    old_string: &str,
    new_string: &str,
) -> Result<String, String> {
    // 1. Exact match (fast path)
    let exact: Vec<usize> = source
        .match_indices(old_string)
        .map(|(pos, _)| pos)
        .collect();

    if exact.len() > 1 {
        return Err(format!(
            "old_string is ambiguous: found {} matches in file. Provide more context.",
            exact.len()
        ));
    }

    if exact.len() == 1 {
        let pos = exact[0];
        return Ok(format!(
            "{}{}{}",
            &source[..pos],
            new_string,
            &source[pos + old_string.len()..]
        ));
    }

    // 2. Indent-normalized matching (with or without ellipsis wildcards)
    if let Some(segments) = split_on_ellipsis(old_string) {
        // Has wildcards — use normalized ellipsis matching
        let matches = find_with_ellipsis(source, &segments);
        if matches.len() == 1 {
            let (start, end) = matches[0];
            let adjusted = adjust_new_indent(source, start, old_string, new_string);
            return Ok(format!("{}{}{}", &source[..start], adjusted, &source[end..]));
        } else if matches.len() > 1 {
            return Err(format!(
                "Pattern is ambiguous: found {} matches in file. Provide more context.",
                matches.len()
            ));
        }
    } else {
        // No wildcards — use normalized line matching
        let matches = find_all_literal_normalized(source, old_string);
        if matches.len() == 1 {
            let (start, end) = matches[0];
            let adjusted = adjust_new_indent(source, start, old_string, new_string);
            return Ok(format!("{}{}{}", &source[..start], adjusted, &source[end..]));
        } else if matches.len() > 1 {
            return Err(format!(
                "Indent-normalized match is ambiguous: found {} matches. Provide more context.",
                matches.len()
            ));
        }
    }

    // 3. Fuzzy matching (only for substantial old_strings)
    let max_total = 10;
    if old_string.len() >= 150 {
        if let Some((offset, matched_len, _dist)) = fuzzy_find(source, old_string, max_total) {
            return Ok(format!(
                "{}{}{}",
                &source[..offset],
                new_string,
                &source[offset + matched_len..]
            ));
        }
    }

    Err("old_string not found in file (no exact, ellipsis, indent-normalized, or fuzzy match)".to_string())
}

/// Scoped edit: find `old_string` within the function's source text and replace it.
/// Matching ignores leading/trailing whitespace per line (indent-insensitive).
/// Supports ellipsis wildcards (`...`, `{ ... }`) and fuzzy matching.
pub fn edit_fn(
    source: &str,
    name: &str,
    old_string: &str,
    new_string: &str,
) -> Result<String, String> {
    let items = parse_file(source)?;
    let found = find_fn(&items, name)?;
    let fn_text = &source[found.start_byte..found.end_byte];

    // 1. Exact match (fast path)
    let exact: Vec<usize> = fn_text
        .match_indices(old_string)
        .map(|(pos, _)| pos)
        .collect();

    if exact.len() > 1 {
        return Err(format!(
            "old_string is ambiguous: found {} matches within function '{}'. Provide a larger snippet for uniqueness.",
            exact.len(),
            name
        ));
    }

    if exact.len() == 1 {
        let match_pos = exact[0];
        let new_fn_text = format!(
            "{}{}{}",
            &fn_text[..match_pos],
            new_string,
            &fn_text[match_pos + old_string.len()..]
        );
        return Ok(format!(
            "{}{}{}",
            &source[..found.start_byte],
            new_fn_text,
            &source[found.end_byte..]
        ));
    }

    // 2. Indent-normalized matching (with or without ellipsis wildcards)
    if let Some(segments) = split_on_ellipsis(old_string) {
        let matches = find_with_ellipsis(fn_text, &segments);
        if matches.len() == 1 {
            let (start, end) = matches[0];
            let adjusted = adjust_new_indent(fn_text, start, old_string, new_string);
            let new_fn_text = format!("{}{}{}", &fn_text[..start], adjusted, &fn_text[end..]);
            return Ok(format!(
                "{}{}{}",
                &source[..found.start_byte],
                new_fn_text,
                &source[found.end_byte..]
            ));
        } else if matches.len() > 1 {
            return Err(format!(
                "Pattern is ambiguous: found {} matches within function '{}'. Provide more context.",
                matches.len(), name
            ));
        }
    } else {
        let matches = find_all_literal_normalized(fn_text, old_string);
        if matches.len() == 1 {
            let (start, end) = matches[0];
            let adjusted = adjust_new_indent(fn_text, start, old_string, new_string);
            let new_fn_text = format!("{}{}{}", &fn_text[..start], adjusted, &fn_text[end..]);
            return Ok(format!(
                "{}{}{}",
                &source[..found.start_byte],
                new_fn_text,
                &source[found.end_byte..]
            ));
        } else if matches.len() > 1 {
            return Err(format!(
                "Indent-normalized match is ambiguous: found {} matches within function '{}'. Provide more context.",
                matches.len(), name
            ));
        }
    }

    // 3. Fuzzy matching (only for substantial old_strings)
    let max_total = 10;
    if old_string.len() >= 150 {
        if let Some((offset, matched_len, _dist)) = fuzzy_find(fn_text, old_string, max_total) {
            let new_fn_text = format!(
                "{}{}{}",
                &fn_text[..offset],
                new_string,
                &fn_text[offset + matched_len..]
            );
            return Ok(format!(
                "{}{}{}",
                &source[..found.start_byte],
                new_fn_text,
                &source[found.end_byte..]
            ));
        }
    }

    Err(format!(
        "old_string not found within function '{}' (no exact, indent-normalized, or fuzzy match)",
        name
    ))
}

/// Replace an entire function's source code with new code.
pub fn replace_fn(source: &str, name: &str, new_fn_source: &str) -> Result<String, String> {
    let items = parse_file(source)?;
    let found = find_fn(&items, name)?;

    Ok(format!(
        "{}{}{}",
        &source[..found.start_byte],
        new_fn_source,
        &source[found.end_byte..]
    ))
}

/// Delete a function by name, including preceding doc comments and cleanup.
pub fn delete_fn(source: &str, name: &str) -> Result<String, String> {
    let items = parse_file(source)?;
    let found = find_fn(&items, name)?;

    // Extend start backwards to capture doc comments and preceding blank line
    let mut start = found.start_byte;
    let before = &source[..start];
    let lines: Vec<&str> = before.lines().collect();
    let mut k = lines.len();
    while k > 0 {
        k -= 1;
        let line = lines[k].trim();
        if line.starts_with("///") || line.starts_with("#[") {
            // Include this line
            start = before
                .rfind(lines[k])
                .map(|pos| {
                    // Find the actual start of this line
                    before[..pos]
                        .rfind('\n')
                        .map(|nl| nl + 1)
                        .unwrap_or(0)
                })
                .unwrap_or(start);
        } else if line.is_empty() {
            // Include one blank line before doc comments
            start = before[..before.len().saturating_sub(1)]
                .rfind('\n')
                .map(|nl| nl + 1)
                .unwrap_or(0);
            break;
        } else {
            break;
        }
    }

    let mut end = found.end_byte;
    // Include trailing newline if present
    if end < source.len() && source.as_bytes()[end] == b'\n' {
        end += 1;
    }

    let mut result = format!("{}{}", &source[..start], &source[end..]);

    // Clean up double blank lines
    while result.contains("\n\n\n") {
        result = result.replace("\n\n\n", "\n\n");
    }

    Ok(result)
}

/// Add a function to the source. If it's a verus function (spec/proof/exec),
/// it goes inside a verus! block. If `after` is specified, insert after that
/// function.
pub fn add_fn(source: &str, new_fn_source: &str, after: Option<&str>) -> Result<String, String> {
    let items = parse_file(source)?;
    let is_verus = detect_verus_fn(new_fn_source);

    if is_verus {
        add_verus_fn(source, new_fn_source, after, &items)
    } else {
        add_regular_fn(source, new_fn_source, after, &items)
    }
}

/// Add a function inside a trait or impl block, identified by name.
/// `inside` matches against trait name, type name, or "Trait for Type" signature.
pub fn add_fn_inside(
    source: &str,
    new_fn_source: &str,
    inside: &str,
    after: Option<&str>,
) -> Result<String, String> {
    let items = parse_file(source)?;

    // If `after` is provided, find that function and insert after it
    // (only if it's inside the target block)
    if let Some(after_name) = after {
        if let Ok(after_fn) = find_fn(&items, after_name) {
            let insert_pos = after_fn.end_byte;
            // Detect indentation from after_fn
            let line_start = source[..after_fn.start_byte].rfind('\n').map(|p| p + 1).unwrap_or(0);
            let indent: String = source[line_start..]
                .chars()
                .take_while(|c| c.is_whitespace())
                .collect();
            let indented: String = new_fn_source
                .lines()
                .enumerate()
                .map(|(i, line)| {
                    if i == 0 { format!("{}{}", indent, line) } else if line.is_empty() { String::new() } else { format!("{}{}", indent, line) }
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Ok(format!(
                "{}\n\n{}{}",
                &source[..insert_pos],
                indented,
                &source[insert_pos..]
            ));
        }
    }

    let inside_stripped = strip_generics(inside);

    // Search traits first
    for tr in &items.traits {
        let tr_stripped = strip_generics(&tr.name);
        if tr.name == inside || tr_stripped == inside_stripped {
            return insert_before_closing_brace(source, tr.end_byte, new_fn_source);
        }
    }

    // Search impls: match type_name, or "Trait for Type" pattern
    for im in &items.impls {
        let im_type_stripped = strip_generics(&im.type_name);
        let sig_match = if let Some(ref trait_name) = im.trait_name {
            let pattern = format!("{} for {}", strip_generics(trait_name), im_type_stripped);
            pattern == inside_stripped || strip_generics(trait_name) == inside_stripped
        } else {
            false
        };
        if im.type_name == inside
            || im_type_stripped == inside_stripped
            || sig_match
        {
            return insert_before_closing_brace(source, im.end_byte, new_fn_source);
        }
    }

    // List available targets
    let mut targets = Vec::new();
    for tr in &items.traits {
        targets.push(format!("trait {}", tr.name));
    }
    for im in &items.impls {
        targets.push(im.signature.clone());
    }
    Err(format!(
        "No trait or impl matching '{}' found. Available:\n{}",
        inside,
        if targets.is_empty() { "  (none)".to_string() } else { targets.iter().map(|t| format!("  {}", t)).collect::<Vec<_>>().join("\n") }
    ))
}

/// Insert source before the closing `}` of a block ending at `end_byte`.
fn insert_before_closing_brace(
    source: &str,
    end_byte: usize,
    new_fn_source: &str,
) -> Result<String, String> {
    // Find the closing `}` — it's at end_byte - 1 (or nearby with trailing whitespace)
    let close_pos = source[..end_byte]
        .rfind('}')
        .ok_or_else(|| "Could not find closing brace of block".to_string())?;

    // Detect indentation from the block body
    let before_close = &source[..close_pos];
    let last_newline = before_close.rfind('\n').unwrap_or(0);
    let existing_content = before_close[last_newline..].trim();

    // Find the indentation used in the block (look at existing methods or use 4 spaces)
    let indent = detect_block_indent(source, close_pos);

    let indented: String = new_fn_source
        .lines()
        .enumerate()
        .map(|(_, line)| {
            if line.is_empty() {
                String::new()
            } else {
                format!("{}{}", indent, line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let needs_newline = !existing_content.is_empty();
    let prefix = if needs_newline { "\n\n" } else { "" };

    Ok(format!(
        "{}{}{}{}{}",
        &source[..close_pos],
        prefix,
        indented,
        "\n",
        &source[close_pos..]
    ))
}

/// Detect indentation level used inside a block by looking at lines before close_pos.
fn detect_block_indent(source: &str, close_pos: usize) -> String {
    // Walk backwards from close_pos to find a non-empty line inside the block
    for line in source[..close_pos].lines().rev() {
        let trimmed = line.trim();
        if !trimmed.is_empty() && (trimmed.starts_with("fn ")
            || trimmed.starts_with("spec fn ")
            || trimmed.starts_with("proof fn ")
            || trimmed.starts_with("exec fn ")
            || trimmed.starts_with("open spec fn ")
            || trimmed.starts_with("pub fn ")
            || trimmed.starts_with("pub spec fn ")
            || trimmed.starts_with("pub proof fn ")
            || trimmed.starts_with("pub exec fn ")
            || trimmed.starts_with("pub open spec fn "))
        {
            let leading: String = line.chars().take_while(|c| c.is_whitespace()).collect();
            return leading;
        }
    }
    "    ".to_string() // default 4 spaces
}

/// List all use statements in a file.
pub fn list_uses(source: &str) -> Result<String, String> {
    let items = parse_file(source)?;
    if items.uses.is_empty() {
        return Ok("No use statements found.".to_string());
    }
    let lines: Vec<&str> = items.uses.iter().map(|u| u.full_text.as_str()).collect();
    Ok(lines.join("\n"))
}

/// Add a use statement. If path has no `::`, it needs to be resolved by the
/// caller (server handler). This function handles the raw insertion.
pub fn add_use(source: &str, use_path: &str) -> Result<String, String> {
    let items = parse_file(source)?;

    // Build the use statement
    let use_stmt = if use_path.starts_with("use ") {
        use_path.to_string()
    } else {
        format!("use {};", use_path)
    };

    // Check for duplicates
    for u in &items.uses {
        if u.full_text.trim() == use_stmt.trim()
            || u.full_text.trim_end_matches(';').trim() == use_path.trim_end_matches(';').trim()
        {
            return Err(format!("Use statement already exists: {}", u.full_text));
        }
    }

    // Insert after last use declaration, or at top if none
    if let Some(last_use) = items.uses.last() {
        let insert_pos = last_use.end_byte;
        // Find end of line
        let line_end = source[insert_pos..]
            .find('\n')
            .map(|p| insert_pos + p + 1)
            .unwrap_or(insert_pos);
        Ok(format!(
            "{}{}\n{}",
            &source[..line_end],
            use_stmt,
            &source[line_end..]
        ))
    } else {
        // No existing use statements — insert at top of file
        Ok(format!("{}\n\n{}", use_stmt, source))
    }
}

/// Remove a use statement by path substring match.
pub fn remove_use(source: &str, path: &str) -> Result<String, String> {
    let items = parse_file(source)?;

    let found: Vec<&LocatedUse> = items
        .uses
        .iter()
        .filter(|u| u.path.contains(path) || u.full_text.contains(path))
        .collect();

    if found.is_empty() {
        return Err(format!("No use statement matching '{}' found", path));
    }
    if found.len() > 1 {
        let matches: Vec<&str> = found.iter().map(|u| u.full_text.as_str()).collect();
        return Err(format!(
            "Ambiguous: {} use statements match '{}':\n{}",
            found.len(),
            path,
            matches.join("\n")
        ));
    }

    let u = found[0];
    let mut start = u.start_byte;
    let mut end = u.end_byte;

    // Include trailing newline
    if end < source.len() && source.as_bytes()[end] == b'\n' {
        end += 1;
    }

    // If there's a blank line before, consume it
    if start > 0 && source.as_bytes()[start - 1] == b'\n' {
        // Check if the line before is also blank
        let before_start = source[..start - 1].rfind('\n').map(|p| p + 1).unwrap_or(0);
        if source[before_start..start - 1].trim().is_empty() {
            start = before_start;
        }
    }

    let mut result = format!("{}{}", &source[..start], &source[end..]);
    while result.contains("\n\n\n") {
        result = result.replace("\n\n\n", "\n\n");
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Strip generic parameters from a name: `Foo<A, B>::bar` → `Foo::bar`
fn strip_generics(s: &str) -> String {
    let mut result = String::new();
    let mut depth = 0usize;
    for ch in s.chars() {
        if ch == '<' {
            depth += 1;
        } else if ch == '>' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 {
            result.push(ch);
        }
    }
    result
}

/// Find a function by name or qualified name. Returns an error if not found
/// or if ambiguous.
fn find_fn<'a>(items: &'a FileItems, name: &str) -> Result<&'a LocatedFn, String> {
    // Normalize "impl Trait for Type::method" or "Trait for Type::method" → "Type::method"
    let name = {
        let s = name.strip_prefix("impl ").unwrap_or(name);
        if let Some(after_for) = s.split_once(" for ").map(|(_, r)| r) {
            after_for
        } else if s != name && s.contains("::") {
            // "impl Type::method" (no "for")
            s
        } else {
            name
        }
    };

    let name_stripped = strip_generics(name);

    let matches: Vec<&LocatedFn> = items
        .functions
        .iter()
        .filter(|f| {
            f.name == name
                || f.qualified_name == name
                || strip_generics(&f.qualified_name) == name_stripped
        })
        .collect();

    match matches.len() {
        0 => {
            let available: Vec<&str> = items.functions.iter().map(|f| f.qualified_name.as_str()).collect();
            Err(format!(
                "Function '{}' not found. Available functions: {}",
                name,
                if available.is_empty() {
                    "(none)".to_string()
                } else {
                    available.join(", ")
                }
            ))
        }
        1 => Ok(matches[0]),
        _ => {
            // If searching by bare name, check if exactly one matches qualified
            let qualified: Vec<&LocatedFn> = matches
                .iter()
                .filter(|f| f.qualified_name == name)
                .copied()
                .collect();
            if qualified.len() == 1 {
                return Ok(qualified[0]);
            }

            // If all matches have the same qualified name (duplicates), return the last one
            let all_same = matches.iter().all(|f| f.qualified_name == matches[0].qualified_name);
            if all_same {
                return Ok(matches.last().unwrap());
            }

            let names: Vec<&str> = matches.iter().map(|f| f.qualified_name.as_str()).collect();
            Err(format!(
                "Ambiguous: '{}' matches {} functions. Use a qualified name: {}",
                name,
                matches.len(),
                names.join(", ")
            ))
        }
    }
}

/// Detect if new function source is a verus function (has spec/proof/exec modifiers).
fn detect_verus_fn(fn_source: &str) -> bool {
    // Try tree-sitter first
    if let Ok(items) = parse_file(fn_source) {
        if let Some(f) = items.functions.first() {
            return f.kind.is_some();
        }
    }
    // Also try wrapping in verus! {}
    let wrapped = format!("verus! {{\n{}\n}}", fn_source);
    if let Ok(items) = parse_file(&wrapped) {
        if let Some(f) = items.functions.first() {
            return f.kind.is_some();
        }
    }
    // Regex fallback
    let re = regex::Regex::new(r"(?:pub\s+)?(?:open\s+)?(?:spec|proof|exec)\s+fn\s").unwrap();
    re.is_match(fn_source)
}

/// Add a verus function inside a verus! block.
fn add_verus_fn(
    source: &str,
    new_fn_source: &str,
    after: Option<&str>,
    items: &FileItems,
) -> Result<String, String> {
    if let Some(after_name) = after {
        // Insert after a specific function
        let after_fn = find_fn(items, after_name)?;

        // Check if after_fn is inside a verus block
        let in_verus = items.verus_blocks.iter().any(|vb| {
            after_fn.start_byte >= vb.body_start_byte && after_fn.end_byte <= vb.body_end_byte
        });

        if in_verus {
            let insert_pos = after_fn.end_byte;
            return Ok(format!(
                "{}\n\n{}{}",
                &source[..insert_pos],
                new_fn_source,
                &source[insert_pos..]
            ));
        }
    }

    if let Some(vb) = items.verus_blocks.first() {
        // Append before the closing `}` of the verus block body
        let insert_pos = vb.body_end_byte;
        // Ensure proper spacing
        let before = &source[..insert_pos];
        let needs_newline = !before.ends_with('\n');
        let prefix = if needs_newline { "\n\n" } else { "\n" };

        Ok(format!(
            "{}{}{}{}",
            &source[..insert_pos],
            prefix,
            new_fn_source,
            &source[insert_pos..]
        ))
    } else {
        // No verus! block exists — create one after all use statements
        let insert_pos = if let Some(last_use) = items.uses.last() {
            // After the last use statement
            let after_use = last_use.end_byte;
            source[after_use..]
                .find('\n')
                .map(|p| after_use + p + 1)
                .unwrap_or(after_use)
        } else {
            0
        };

        let verus_block = format!(
            "\nverus! {{\n\n{}\n\n}} // verus!\n",
            new_fn_source
        );

        Ok(format!(
            "{}{}{}",
            &source[..insert_pos],
            verus_block,
            &source[insert_pos..]
        ))
    }
}

/// Add a regular (non-verus) function outside verus! blocks.
fn add_regular_fn(
    source: &str,
    new_fn_source: &str,
    after: Option<&str>,
    items: &FileItems,
) -> Result<String, String> {
    if let Some(after_name) = after {
        let after_fn = find_fn(items, after_name)?;
        let insert_pos = after_fn.end_byte;
        return Ok(format!(
            "{}\n\n{}{}",
            &source[..insert_pos],
            new_fn_source,
            &source[insert_pos..]
        ));
    }

    // Append at end of file
    let trimmed = source.trim_end();
    Ok(format!("{}\n\n{}\n", trimmed, new_fn_source))
}
