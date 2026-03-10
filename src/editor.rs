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
    let mut output = Vec::new();

    // Functions (optionally filtered)
    for f in &items.functions {
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
            let kind_label = match f.kind {
                Some(k) => format!("[{}] ", k),
                None => "[fn] ".to_string(),
            };
            output.push(format!("{}{}", kind_label, f.signature));
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
                // already handled above
            };
            if include {
                output.push(format!("[{}] {}", t.kind, t.signature));
            }
        }
    }

    // Traits
    if kind_filter.is_none() || kind_filter == Some("trait") {
        for t in &items.traits {
            output.push(format!("[trait] {}", t.signature));
        }
    }

    // Impls
    if kind_filter.is_none() || kind_filter == Some("impl") {
        for im in &items.impls {
            let methods: Vec<&str> = im.methods.iter().map(|m| m.name.as_str()).collect();
            if methods.is_empty() {
                output.push(format!("[impl] {}", im.signature));
            } else {
                output.push(format!(
                    "[impl] {} {{ {} }}",
                    im.signature,
                    methods.join(", ")
                ));
            }
        }
    }

    if output.is_empty() {
        Ok("No items found.".to_string())
    } else {
        Ok(output.join("\n"))
    }
}

/// Return the source text of a function by name.
/// Supports qualified names like "Type::method".
pub fn read_fn(source: &str, name: &str) -> Result<String, String> {
    let items = parse_file(source)?;
    let found = find_fn(&items, name)?;
    Ok(source[found.start_byte..found.end_byte].to_string())
}

/// Scoped edit: find `old_string` within the function's source text and replace it.
pub fn edit_fn(
    source: &str,
    name: &str,
    old_string: &str,
    new_string: &str,
) -> Result<String, String> {
    let items = parse_file(source)?;
    let found = find_fn(&items, name)?;
    let fn_text = &source[found.start_byte..found.end_byte];

    // Find old_string within the function
    let matches: Vec<usize> = fn_text
        .match_indices(old_string)
        .map(|(pos, _)| pos)
        .collect();

    if matches.is_empty() {
        return Err(format!(
            "old_string not found within function '{}'",
            name
        ));
    }
    if matches.len() > 1 {
        return Err(format!(
            "old_string is ambiguous: found {} matches within function '{}'. Provide a larger snippet for uniqueness.",
            matches.len(),
            name
        ));
    }

    let match_pos = matches[0];
    let new_fn_text = format!(
        "{}{}{}",
        &fn_text[..match_pos],
        new_string,
        &fn_text[match_pos + old_string.len()..]
    );

    Ok(format!(
        "{}{}{}",
        &source[..found.start_byte],
        new_fn_text,
        &source[found.end_byte..]
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

/// Find a function by name or qualified name. Returns an error if not found
/// or if ambiguous.
fn find_fn<'a>(items: &'a FileItems, name: &str) -> Result<&'a LocatedFn, String> {
    let matches: Vec<&LocatedFn> = items
        .functions
        .iter()
        .filter(|f| f.name == name || f.qualified_name == name)
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
