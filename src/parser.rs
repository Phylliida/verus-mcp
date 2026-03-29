use crate::types::*;

///  Parsed output from a single file.
#[derive(Debug, Clone, Default)]
pub struct ParsedItems {
    pub functions: Vec<FnEntry>,
    pub types: Vec<TypeEntry>,
    pub traits: Vec<TraitEntry>,
    pub impls: Vec<ImplEntry>,
}

///  Extract function and type entries from a Verus source file using tree-sitter.
pub fn extract_items(
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
) -> Result<ParsedItems, String> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_verus::LANGUAGE.into())
        .map_err(|e| format!("Failed to load Verus grammar: {}", e))?;

    let tree = parser
        .parse(source.as_bytes(), None)
        .ok_or_else(|| "Failed to parse source".to_string())?;

    let root = tree.root_node();
    let mut items = ParsedItems::default();

    collect_items_from_node(&root, source, file_path, crate_name, module_path, None, &mut items);

    Ok(items)
}

fn collect_items_from_node(
    node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
    trait_name: Option<&str>,
    items: &mut ParsedItems,
) {
    //  If this node itself is an ERROR, extract orphaned function signatures from it.
    //  This handles the case where the root or a top-level node is ERROR (e.g. when
    //  verus! {} causes the entire file to parse as one big ERROR node).
    if node.kind() == "ERROR" {
        extract_orphaned_functions(node, source, file_path, crate_name, module_path, items);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "verus_block" => {
                //  verus! { ... } — body field contains a declaration_list
                if let Some(body) = child.child_by_field_name("body") {
                    collect_items_from_node(
                        &body, source, file_path, crate_name, module_path, trait_name, items,
                    );
                }
            }
            "declaration_list" => {
                collect_items_from_node(
                    &child, source, file_path, crate_name, module_path, trait_name, items,
                );
            }
            "impl_item" => {
                collect_items_from_impl(
                    &child, source, file_path, crate_name, module_path, items,
                );
            }
            "trait_item" => {
                collect_items_from_trait(
                    &child, source, file_path, crate_name, module_path, items,
                );
            }
            "function_item" | "function_signature_item" => {
                if let Some(item) = extract_function_item(
                    &child, source, file_path, crate_name, module_path, trait_name,
                ) {
                    items.functions.push(item);
                }
            }
            "struct_item" => {
                if let Some(entry) = extract_struct_item(
                    &child, source, file_path, crate_name, module_path,
                ) {
                    items.types.push(entry);
                }
            }
            "enum_item" => {
                if let Some(entry) = extract_enum_item(
                    &child, source, file_path, crate_name, module_path,
                ) {
                    items.types.push(entry);
                }
            }
            "type_item" => {
                if let Some(entry) = extract_type_alias(
                    &child, source, file_path, crate_name, module_path,
                ) {
                    items.types.push(entry);
                }
            }
            //  Recurse into ERROR, block, and expression_statement nodes.
            //  verus! { ... } creates ERROR + block during error recovery;
            //  function_items inside need to be found recursively.
            "ERROR" | "block" | "expression_statement" => {
                collect_items_from_node(
                    &child, source, file_path, crate_name, module_path, trait_name, items,
                );
                //  Also extract orphaned function signatures from ERROR nodes.
                //  When tree-sitter fails to form a function_item (e.g. for very long
                //  functions inside verus! blocks), the signature components appear as
                //  siblings: visibility_modifier, function_modifiers, identifier, etc.
                if child.kind() == "ERROR" {
                    extract_orphaned_functions(
                        &child, source, file_path, crate_name, module_path, items,
                    );
                }
            }
            _ => {}
        }
    }
}

fn collect_items_from_impl(
    impl_node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
    items: &mut ParsedItems,
) {
    let type_name = impl_node
        .child_by_field_name("type")
        .map(|n| node_text(&n, source))
        .unwrap_or_default();

    //  Trait name from `impl Trait for Type` — the "trait" field
    let trait_name = impl_node
        .child_by_field_name("trait")
        .map(|n| node_text(&n, source));

    let type_params = impl_node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));

    let impl_module = if type_name.is_empty() {
        module_path.to_string()
    } else {
        format!("{}::{}", module_path, type_name)
    };

    let mut method_names = Vec::new();

    if let Some(body) = impl_node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "function_item" || child.kind() == "function_signature_item" {
                if let Some(name_node) = child.child_by_field_name("name") {
                    method_names.push(node_text(&name_node, source));
                }
                if let Some(item) = extract_function_item(
                    &child, source, file_path, crate_name, &impl_module, None,
                ) {
                    items.functions.push(item);
                }
            }
        }
    }

    let line = impl_node.start_position().row + 1;
    let end_line = impl_node.end_position().row + 1;
    items.impls.push(ImplEntry {
        trait_name,
        type_name,
        type_params,
        method_names,
        crate_name: crate_name.to_string(),
        module_path: module_path.to_string(),
        file_path: file_path.to_string(),
        line,
        end_line,
    });
}

fn collect_items_from_trait(
    trait_node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
    items: &mut ParsedItems,
) {
    let trait_name = trait_node
        .child_by_field_name("name")
        .map(|n| node_text(&n, source))
        .unwrap_or_default();

    let visibility = extract_visibility(trait_node, source);
    let type_params = trait_node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));
    let doc_comment = extract_doc_comment(trait_node, source);
    let line = trait_node.start_position().row + 1;
    let end_line = trait_node.end_position().row + 1;

    //  Extract supertraits from bounds (e.g., `trait Foo: Bar + Baz`)
    let supertraits = trait_node
        .child_by_field_name("bounds")
        .map(|n| node_text(&n, source));

    let trait_module = if trait_name.is_empty() {
        module_path.to_string()
    } else {
        format!("{}::{}", module_path, trait_name)
    };

    let mut method_names = Vec::new();

    if let Some(body) = trait_node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "function_item" || child.kind() == "function_signature_item" {
                if let Some(name_node) = child.child_by_field_name("name") {
                    method_names.push(node_text(&name_node, source));
                }
                let tname = if trait_name.is_empty() {
                    None
                } else {
                    Some(trait_name.as_str())
                };
                if let Some(item) = extract_function_item(
                    &child, source, file_path, crate_name, &trait_module, tname,
                ) {
                    items.functions.push(item);
                }
            }
        }
    }

    if !trait_name.is_empty() {
        items.traits.push(TraitEntry {
            name: trait_name,
            visibility,
            type_params,
            supertraits,
            method_names,
            crate_name: crate_name.to_string(),
            module_path: module_path.to_string(),
            doc_comment,
            file_path: file_path.to_string(),
            line,
            end_line,
        });
    }
}

fn extract_function_item(
    node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
    trait_name: Option<&str>,
) -> Option<FnEntry> {
    let name = node.child_by_field_name("name")?;
    let name_text = node_text(&name, source);

    let visibility = extract_visibility(node, source);
    let (kind, is_open) = extract_fn_kind(node, source);
    let line = node.start_position().row + 1;
    let end_line = node.end_position().row + 1;
    let doc_comment = extract_doc_comment(node, source);

    //  Type parameters
    let type_params = node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));

    //  Parameters
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(&n, source))
        .unwrap_or_else(|| "()".to_string());

    //  Return type
    let return_type = node
        .child_by_field_name("return_type")
        .map(|n| node_text(&n, source));

    //  Requires and ensures clauses
    let requires = extract_clause(node, source, "requires_clause");
    let ensures = extract_clause(node, source, "ensures_clause");

    //  Body text (for function_item, not function_signature_item)
    let body = node
        .child_by_field_name("body")
        .map(|n| node_text(&n, source));

    Some(FnEntry {
        name: name_text,
        kind,
        visibility,
        is_open,
        type_params,
        params,
        return_type,
        requires,
        ensures,
        crate_name: crate_name.to_string(),
        module_path: module_path.to_string(),
        trait_name: trait_name.map(|s| s.to_string()),
        doc_comment,
        body,
        file_path: file_path.to_string(),
        line,
        end_line,
    })
}

///  Extract function entries from orphaned signature components inside ERROR nodes.
///  When tree-sitter can't form a `function_item` (e.g. for very long functions inside
///  `verus!` blocks), the components (visibility_modifier, function_modifiers, identifier,
///  type_parameters, parameters, ensures_clause, decreases_clause) appear as siblings
///  in the ERROR node rather than children of a function_item.
fn extract_orphaned_functions(
    error_node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
    items: &mut ParsedItems,
) {
    let child_count = error_node.child_count();
    //  Collect names of already-found function_items to avoid duplicates
    let existing: std::collections::HashSet<String> = items
        .functions
        .iter()
        .map(|f| f.name.clone())
        .collect();

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

        //  Found function_modifiers — try to extract an orphaned function signature.
        let mods_node = child;
        let mods_idx = i;

        //  Extract kind from function_modifiers
        let mut kind = FnKind::Exec;
        let mut is_open = false;
        {
            let mut mc = mods_node.walk();
            for modifier in mods_node.children(&mut mc) {
                match node_text(&modifier, source).as_str() {
                    "spec" => kind = FnKind::Spec,
                    "proof" => kind = FnKind::Proof,
                    "exec" => kind = FnKind::Exec,
                    "open" => is_open = true,
                    _ => {}
                }
            }
        }

        //  Check for visibility_modifier immediately before
        let visibility = if mods_idx > 0 {
            match error_node.child(mods_idx - 1) {
                Some(prev) if prev.kind() == "visibility_modifier" => {
                    let text = node_text(&prev, source);
                    if text.contains("crate") {
                        Visibility::PublicCrate
                    } else {
                        Visibility::Public
                    }
                }
                _ => Visibility::Private,
            }
        } else {
            Visibility::Private
        };

        //  Scan forward for identifier (function name), skipping anonymous nodes like 'fn'
        let mut j = mods_idx + 1;
        let mut name_text: Option<String> = None;
        while j < child_count {
            if let Some(n) = error_node.child(j) {
                if n.kind() == "identifier" {
                    name_text = Some(node_text(&n, source));
                    j += 1;
                    break;
                }
                //  Stop if we hit structural markers of another function
                if n.kind() == "function_modifiers" || n.kind() == "function_item" {
                    break;
                }
                //  Skip anonymous nodes (like 'fn' keyword)
                if !n.is_named() || n.kind() == "visibility_modifier" {
                    j += 1;
                    continue;
                }
                //  If it's a named node that isn't identifier, stop
                break;
            } else {
                break;
            }
        }

        let name_text = match name_text {
            Some(n) => n,
            None => { i = j.max(mods_idx + 1); continue; }
        };

        //  Skip if already found via normal function_item extraction
        if existing.contains(name_text.as_str()) {
            i = j;
            continue;
        }

        //  Collect optional components following the name
        let mut type_params = None;
        let mut params = "()".to_string();
        let mut return_type = None;
        let mut requires = Vec::new();
        let mut ensures = Vec::new();
        let mut body_text = None;
        let start_line = if mods_idx > 0 {
            error_node.child(mods_idx - 1)
                .filter(|n| n.kind() == "visibility_modifier")
                .map(|n| n.start_position().row + 1)
                .unwrap_or_else(|| mods_node.start_position().row + 1)
        } else {
            mods_node.start_position().row + 1
        };
        let mut end_line = mods_node.end_position().row + 1;

        //  Scan forward collecting signature components and body
        while j < child_count {
            let n = match error_node.child(j) {
                Some(n) => n,
                None => break,
            };
            match n.kind() {
                "type_parameters" => {
                    type_params = Some(node_text(&n, source));
                    end_line = n.end_position().row + 1;
                }
                "parameters" => {
                    params = node_text(&n, source);
                    end_line = n.end_position().row + 1;
                }
                "return_type" | "type_identifier" if return_type.is_none() && n.start_position().row == mods_node.start_position().row => {
                    //  Return type on the same line as the signature
                    return_type = Some(node_text(&n, source));
                    end_line = n.end_position().row + 1;
                }
                "requires_clause" => {
                    requires = extract_clause_from_sibling(&n, source);
                    end_line = n.end_position().row + 1;
                }
                "ensures_clause" => {
                    ensures = extract_clause_from_sibling(&n, source);
                    end_line = n.end_position().row + 1;
                }
                "decreases_clause" => {
                    end_line = n.end_position().row + 1;
                }
                //  Body content: update end_line but don't collect body text
                //  (the body isn't a clean block for orphaned functions)
                "let_declaration" | "expression_statement" | "block" => {
                    end_line = n.end_position().row + 1;
                    //  If it's a block that might be the whole function body
                    if n.kind() == "block" && body_text.is_none() {
                        body_text = Some(node_text(&n, source));
                    }
                }
                //  Stop at the next function signature
                "visibility_modifier" | "function_modifiers" | "function_item" => break,
                "line_comment" => {
                    //  Comments between functions — keep extending end_line
                    //  but don't break, the body might continue
                    end_line = n.end_position().row + 1;
                }
                _ => {
                    //  Other body nodes — extend end_line
                    end_line = n.end_position().row + 1;
                }
            }
            j += 1;
        }

        //  Extract doc comment from source text above start_line
        let doc_comment = {
            let start_byte = mods_node.start_byte();
            let before = &source[..start_byte];
            let lines: Vec<&str> = before.lines().collect();
            let mut comments = Vec::new();
            let mut k = lines.len();
            while k > 0 {
                k -= 1;
                let line = lines[k].trim();
                if line.starts_with("///") {
                    let doc_text = line.trim_start_matches("///").trim();
                    comments.push(doc_text.to_string());
                } else if line.is_empty() || line.starts_with("#[") {
                    continue;
                } else {
                    break;
                }
            }
            if comments.is_empty() {
                None
            } else {
                comments.reverse();
                Some(comments.join(" "))
            }
        };

        items.functions.push(FnEntry {
            name: name_text,
            kind,
            visibility,
            is_open,
            type_params,
            params,
            return_type,
            requires,
            ensures,
            crate_name: crate_name.to_string(),
            module_path: module_path.to_string(),
            trait_name: None,
            doc_comment,
            body: body_text,
            file_path: file_path.to_string(),
            line: start_line,
            end_line,
        });

        i = j;
        continue;
    }
}

///  Extract clause items directly from a clause node (requires_clause or ensures_clause).
///  Unlike `extract_clause` which searches children of a parent node for the clause,
///  this takes the clause node itself.
fn extract_clause_from_sibling(clause_node: &tree_sitter::Node, source: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut cursor = clause_node.walk();
    for expr in clause_node.children(&mut cursor) {
        let kind = expr.kind();
        if kind == "requires" || kind == "ensures" || kind == "," {
            continue;
        }
        if !expr.is_named() {
            continue;
        }
        let text = node_text(&expr, source).trim().to_string();
        if !text.is_empty() {
            results.push(text);
        }
    }
    results
}

fn extract_struct_item(
    node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
) -> Option<TypeEntry> {
    let name = node.child_by_field_name("name")?;
    let name_text = node_text(&name, source);
    let visibility = extract_visibility(node, source);
    let line = node.start_position().row + 1;
    let end_line = node.end_position().row + 1;
    let doc_comment = extract_doc_comment(node, source);

    let type_params = node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));

    //  Extract field declarations
    let mut fields = Vec::new();
    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "field_declaration" {
                fields.push(node_text(&child, source).trim().to_string());
            }
        }
    }

    Some(TypeEntry {
        name: name_text,
        item_kind: ItemKind::Struct,
        visibility,
        type_params,
        fields,
        aliased_type: None,
        crate_name: crate_name.to_string(),
        module_path: module_path.to_string(),
        doc_comment,
        file_path: file_path.to_string(),
        line,
        end_line,
    })
}

fn extract_enum_item(
    node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
) -> Option<TypeEntry> {
    let name = node.child_by_field_name("name")?;
    let name_text = node_text(&name, source);
    let visibility = extract_visibility(node, source);
    let line = node.start_position().row + 1;
    let end_line = node.end_position().row + 1;
    let doc_comment = extract_doc_comment(node, source);

    let type_params = node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));

    //  Extract variant names
    let mut fields = Vec::new();
    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "enum_variant" {
                //  Get just the variant name (first named child)
                if let Some(vname) = child.child_by_field_name("name") {
                    fields.push(node_text(&vname, source));
                } else {
                    fields.push(node_text(&child, source).trim().to_string());
                }
            }
        }
    }

    Some(TypeEntry {
        name: name_text,
        item_kind: ItemKind::Enum,
        visibility,
        type_params,
        fields,
        aliased_type: None,
        crate_name: crate_name.to_string(),
        module_path: module_path.to_string(),
        doc_comment,
        file_path: file_path.to_string(),
        line,
        end_line,
    })
}

fn extract_type_alias(
    node: &tree_sitter::Node,
    source: &str,
    file_path: &str,
    crate_name: &str,
    module_path: &str,
) -> Option<TypeEntry> {
    let name = node.child_by_field_name("name")?;
    let name_text = node_text(&name, source);
    let visibility = extract_visibility(node, source);
    let line = node.start_position().row + 1;
    let end_line = node.end_position().row + 1;
    let doc_comment = extract_doc_comment(node, source);

    let type_params = node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));

    //  The aliased type (right-hand side of =)
    let aliased_type = node
        .child_by_field_name("type")
        .map(|n| node_text(&n, source));

    Some(TypeEntry {
        name: name_text,
        item_kind: ItemKind::TypeAlias,
        visibility,
        type_params,
        fields: Vec::new(),
        aliased_type,
        crate_name: crate_name.to_string(),
        module_path: module_path.to_string(),
        doc_comment,
        file_path: file_path.to_string(),
        line,
        end_line,
    })
}

///  Extract predicates from a requires_clause or ensures_clause node.
fn extract_clause(
    fn_node: &tree_sitter::Node,
    source: &str,
    clause_kind: &str,
) -> Vec<String> {
    let mut results = Vec::new();

    let mut cursor = fn_node.walk();
    for child in fn_node.children(&mut cursor) {
        if child.kind() == clause_kind {
            //  The clause's children are: keyword, then expression nodes separated by commas
            let mut expr_cursor = child.walk();
            for expr in child.children(&mut expr_cursor) {
                let kind = expr.kind();
                //  Skip the keyword itself and comma punctuation
                if kind == "requires" || kind == "ensures" || kind == "," {
                    continue;
                }
                //  Skip anonymous nodes that are just punctuation
                if !expr.is_named() {
                    continue;
                }
                let text = node_text(&expr, source).trim().to_string();
                if !text.is_empty() {
                    results.push(text);
                }
            }
            break;
        }
    }

    results
}

fn extract_visibility(node: &tree_sitter::Node, source: &str) -> Visibility {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "visibility_modifier" {
            let text = node_text(&child, source);
            if text.contains("crate") {
                return Visibility::PublicCrate;
            }
            return Visibility::Public;
        }
    }
    Visibility::Private
}

fn extract_fn_kind(node: &tree_sitter::Node, source: &str) -> (FnKind, bool) {
    let mut kind = FnKind::Exec;
    let mut is_open = false;

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "function_modifiers" {
            let mut mod_cursor = child.walk();
            for modifier in child.children(&mut mod_cursor) {
                match node_text(&modifier, source).as_str() {
                    "spec" => kind = FnKind::Spec,
                    "proof" => kind = FnKind::Proof,
                    "exec" => kind = FnKind::Exec,
                    "open" => is_open = true,
                    _ => {}
                }
            }
        }
    }

    (kind, is_open)
}

fn extract_doc_comment(node: &tree_sitter::Node, source: &str) -> Option<String> {
    let mut comments = Vec::new();
    let mut prev = node.prev_sibling();

    while let Some(sibling) = prev {
        if sibling.kind() == "line_comment" {
            let text = node_text(&sibling, source);
            if text.starts_with("///") {
                let doc_text = text.trim_start_matches("///").trim();
                comments.push(doc_text.to_string());
                prev = sibling.prev_sibling();
                continue;
            }
        } else if sibling.kind() == "attribute_item" {
            prev = sibling.prev_sibling();
            continue;
        }
        break;
    }

    //  Fallback: look at raw source text above the function
    if comments.is_empty() {
        let start_byte = node.start_byte();
        let before = &source[..start_byte];
        let lines: Vec<&str> = before.lines().collect();
        let mut i = lines.len();
        while i > 0 {
            i -= 1;
            let line = lines[i].trim();
            if line.starts_with("///") {
                let doc_text = line.trim_start_matches("///").trim();
                comments.push(doc_text.to_string());
            } else if line.is_empty() || line.starts_with("#[") {
                continue;
            } else {
                break;
            }
        }
    }

    if comments.is_empty() {
        None
    } else {
        comments.reverse();
        Some(comments.join(" "))
    }
}

fn node_text(node: &tree_sitter::Node, source: &str) -> String {
    node.utf8_text(source.as_bytes())
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orphaned_function_extraction() {
        let source = std::fs::read_to_string(
            "/Users/yams/Prog/verus-cad/verus-algebra/src/binomial/mod.rs"
        ).unwrap();

        let items = extract_items(&source, "test.rs", "verus_algebra", "binomial").unwrap();
        let found = items.functions.iter().find(|f| f.name == "lemma_binomial_theorem");
        assert!(found.is_some(), "lemma_binomial_theorem should be found");
        let f = found.unwrap();
        assert_eq!(f.kind, FnKind::Proof);
        assert!(!f.ensures.is_empty(), "should have ensures clause");
    }

    #[test]
    fn test_verus_block_parsing() {
        let mut parser = tree_sitter::Parser::new();
        parser.set_language(&tree_sitter_verus::LANGUAGE.into()).unwrap();

        //  Minimal verus block
        let source = "verus! { }";
        let tree = parser.parse(source.as_bytes(), None).unwrap();
        let root = tree.root_node();
        assert_eq!(root.child(0).unwrap().kind(), "verus_block",
            "minimal verus! should parse as verus_block, got: {}", root.to_sexp());

        //  verus block with function
        let source2 = "verus! {\npub spec fn foo() -> bool { true }\n}";
        let tree2 = parser.parse(source2.as_bytes(), None).unwrap();
        let root2 = tree2.root_node();
        assert_eq!(root2.child(0).unwrap().kind(), "verus_block");

        //  Test with use declarations before verus block
        let source3 = "use vstd::prelude::*;\nuse crate::traits::ring::Ring;\n\nverus! {\n\npub spec fn binom(n: nat, k: nat) -> nat {\n    if k == 0 { 1 }\n    else if k > n { 0 }\n    else { binom(n - 1, k - 1) + binom(n - 1, k) }\n}\n\n}\n";
        let tree3 = parser.parse(source3.as_bytes(), None).unwrap();
        let root3 = tree3.root_node();
        eprintln!("With use+verus: root={}", root3.kind());
        let mut cursor3 = root3.walk();
        for child in root3.children(&mut cursor3) {
            eprintln!("  {} [{}:{}-{}:{}]",
                child.kind(),
                child.start_position().row, child.start_position().column,
                child.end_position().row, child.end_position().column);
        }

        //  Read the real file lines
        let real_source = std::fs::read_to_string(
            "/Users/yams/Prog/verus-cad/verus-algebra/src/binomial/mod.rs"
        ).unwrap();
        let lines: Vec<&str> = real_source.lines().collect();

        //  lines 0-11: use declarations
        //  line 12: verus! {
        //  lines 13-708: content
        //  line 709: } //  verus!

        //  The binary search found line 170 breaks it. Try:
        //  1. Lines 13-169 (works)
        //  2. Lines 13-170 (breaks)
        //  3. Just the function containing line 170 (lines 159-575)

        //  Test the actual full function in verus! block
        let real_source = std::fs::read_to_string(
            "/Users/yams/Prog/verus-cad/verus-algebra/src/binomial/mod.rs"
        ).unwrap();
        let lines: Vec<&str> = real_source.lines().collect();

        //  Just the one function: lines 159-575 (0-indexed, inclusive)
        let mut test = String::new();
        test.push_str("verus! {\n");
        for i in 159..=575 {
            test.push_str(lines[i]);
            test.push('\n');
        }
        test.push_str("}\n");
        let tree = parser.parse(test.as_bytes(), None).unwrap();
        let root = tree.root_node();
        let has_vb = root.children(&mut root.walk()).any(|c| c.kind() == "verus_block");
        eprintln!("Full function: verus_block={}, error={}", has_vb, root.has_error());

        //  Binary search by including chunks and closing all braces properly
        //  Strategy: try just base case (n==0), just else case, etc.
        //  Base case: lines 159-224(ish), Else case starts around 227
        //  Let me check: line 176 = if n == 0 {, line ~225 = } else {
        //  Let me try first half of body only with proper brace closing
        let halves = [
            ("base_case", 159, 226),   //  if n == 0 { ... }
            ("else_case", 159, 575),   //  full function
        ];

        //  Actually, let me just find which source line causes parse errors
        //  by testing individual lines' impact
        //  Test: full function but strip non-ASCII from comments
        let mut test_ascii = String::new();
        test_ascii.push_str("verus! {\n");
        for i in 159..=575 {
            let line = lines[i];
            if line.trim_start().starts_with("//") {
                //  Replace non-ASCII in comments
                let ascii_line: String = line.chars().map(|c| if c.is_ascii() { c } else { '=' }).collect();
                test_ascii.push_str(&ascii_line);
            } else {
                test_ascii.push_str(line);
            }
            test_ascii.push('\n');
        }
        test_ascii.push_str("}\n");
        let tree = parser.parse(test_ascii.as_bytes(), None).unwrap();
        let root = tree.root_node();
        let has_vb = root.children(&mut root.walk()).any(|c| c.kind() == "verus_block");
        eprintln!("ASCII-only comments: verus_block={}, error={}", has_vb, root.has_error());

        //  Find the else line to split the function
        let else_line = (170..575).find(|&i| lines[i].trim() == "} else {").unwrap_or(575);
        eprintln!("else at line {}: {:?}", else_line, lines.get(else_line));

        //  Test: sig + base case only
        let mut test_base = String::new();
        test_base.push_str("verus! {\n");
        for i in 159..=169 { test_base.push_str(lines[i]); test_base.push('\n'); }
        for i in 170..else_line { test_base.push_str(lines[i]); test_base.push('\n'); }
        test_base.push_str("    }\n}\n}\n"); //  close if, fn, verus
        let t = parser.parse(test_base.as_bytes(), None).unwrap();
        eprintln!("Base case only: verus_block={}, error={}",
            t.root_node().children(&mut t.root_node().walk()).any(|c| c.kind() == "verus_block"),
            t.root_node().has_error());

        //  Test: sig + else case only (without the let f)
        let mut test_else = String::new();
        test_else.push_str("verus! {\n");
        for i in 159..=169 { test_else.push_str(lines[i]); test_else.push('\n'); }
        //  skip let f closure, go straight into body
        test_else.push_str("    assert(true);\n");
        test_else.push_str("}\n}\n");
        let t = parser.parse(test_else.as_bytes(), None).unwrap();
        eprintln!("Simple body: verus_block={}, error={}",
            t.root_node().children(&mut t.root_node().walk()).any(|c| c.kind() == "verus_block"),
            t.root_node().has_error());

        //  Binary search in else block: find which line breaks it
        //  else block: lines 228 to 573 (content inside } else { ... })
        //  We need the } to close 'if n == 0' at line 573/574
        let else_content_start = 228;
        let else_content_end = 573; //  line before closing }

        let test_fn = |end_line: usize| -> bool {
            let mut parser2 = tree_sitter::Parser::new();
            parser2.set_language(&tree_sitter_verus::LANGUAGE.into()).unwrap();

            let mut test = String::new();
            test.push_str("verus! {\n");
            for i in 159..=175 { test.push_str(lines[i]); test.push('\n'); }
            test.push_str("    if n == 0 {\n        assert(true);\n    } else {\n");
            for i in else_content_start..=end_line {
                test.push_str(lines[i]);
                test.push('\n');
            }
            test.push_str("    }\n}\n}\n"); //  close else, fn, verus
            let t = parser2.parse(test.as_bytes(), None).unwrap();
            !t.root_node().has_error()
        };

        //  First check: does the full else block fail?
        eprintln!("Full else block: ok={}", test_fn(else_content_end));

        //  Check what errors exist in files that DO parse as verus_block but have errors
        fn find_errors(node: &tree_sitter::Node, source: &str, depth: usize) {
            if node.has_error() {
                let indent = "  ".repeat(depth);
                if node.is_error() {
                    let text: String = node.utf8_text(source.as_bytes()).unwrap_or("?")
                        .chars().take(100).collect();
                    eprintln!("{}ERROR [{}:{}-{}:{}]: {:?}",
                        indent,
                        node.start_position().row, node.start_position().column,
                        node.end_position().row, node.end_position().column,
                        text);
                } else if node.is_missing() {
                    eprintln!("{}MISSING {} [{}:{}]",
                        indent, node.kind(),
                        node.start_position().row, node.start_position().column);
                } else {
                    let mut cursor = node.walk();
                    for child in node.children(&mut cursor) {
                        if child.has_error() {
                            find_errors(&child, source, depth + 1);
                        }
                    }
                }
            }
        }

        //  Test: full signature + full body in verus! block
        let real_source = std::fs::read_to_string(
            "/Users/yams/Prog/verus-cad/verus-algebra/src/binomial/mod.rs"
        ).unwrap();
        let body_lines: Vec<&str> = real_source.lines().collect();

        //  Full function: lines 159-575
        let mut full_fn = String::new();
        full_fn.push_str("verus! {\n");
        for i in 159..=575 {
            full_fn.push_str(body_lines[i]);
            full_fn.push('\n');
        }
        full_fn.push_str("}\n");

        let mut p2 = tree_sitter::Parser::new();
        p2.set_language(&tree_sitter_verus::LANGUAGE.into()).unwrap();
        let t = p2.parse(full_fn.as_bytes(), None).unwrap();
        let root = t.root_node();
        let has_vb = root.children(&mut root.walk()).any(|c| c.kind() == "verus_block");
        eprintln!("Full fn in verus!: verus_block={}, error={}", has_vb, root.has_error());

        //  Simplified signature + full body
        let mut simple_sig = String::new();
        simple_sig.push_str("verus! {\npub proof fn lemma_binomial_theorem<R: Ring>(a: R, b: R, n: nat)\n    ensures true,\n    decreases n,\n{\n");
        for i in 170..=574 {
            simple_sig.push_str(body_lines[i]);
            simple_sig.push('\n');
        }
        simple_sig.push_str("}\n}\n");
        let t2 = p2.parse(simple_sig.as_bytes(), None).unwrap();
        let r2 = t2.root_node();
        eprintln!("Simple sig + full body: verus_block={}, error={}",
            r2.children(&mut r2.walk()).any(|c| c.kind() == "verus_block"),
            r2.has_error());

        //  Test: add ; after assert forall ... by { } blocks
        let real_source = std::fs::read_to_string(
            "/Users/yams/Prog/verus-cad/verus-algebra/src/binomial/mod.rs"
        ).unwrap();
        let body_lines: Vec<&str> = real_source.lines().collect();

        //  Add ; after } that closes assert forall by { } blocks
        let mut patched = String::new();
        patched.push_str("verus! {\n");
        let mut in_assert_by = 0i32;
        for i in 159..=575 {
            let trimmed = body_lines[i].trim();
            if trimmed.starts_with("assert forall") && trimmed.ends_with("by {") {
                patched.push_str(body_lines[i]);
                patched.push('\n');
                in_assert_by = 1;
                continue;
            }
            if in_assert_by > 0 {
                let opens: i32 = body_lines[i].chars().filter(|&c| c == '{').count() as i32;
                let closes: i32 = body_lines[i].chars().filter(|&c| c == '}').count() as i32;
                in_assert_by += opens - closes;
                if in_assert_by <= 0 {
                    //  This is the closing } — add ;
                    patched.push_str(body_lines[i]);
                    patched.push_str(";");
                    patched.push('\n');
                    in_assert_by = 0;
                    continue;
                }
            }
            //  Also handle assert(expr) by { } (single line)
            if trimmed.starts_with("assert(") && trimmed.ends_with("by { };") {
                //  already has ;, skip
            }
            patched.push_str(body_lines[i]);
            patched.push('\n');
        }
        patched.push_str("}\n");

        let mut p3 = tree_sitter::Parser::new();
        p3.set_language(&tree_sitter_verus::LANGUAGE.into()).unwrap();
        let t = p3.parse(patched.as_bytes(), None).unwrap();
        let r = t.root_node();
        eprintln!("Patched (added ;): verus_block={}, error={}",
            r.children(&mut r.walk()).any(|c| c.kind() == "verus_block"),
            r.has_error());
        if r.has_error() {
            find_errors(&r, &patched, 0);
        }
    }
}
