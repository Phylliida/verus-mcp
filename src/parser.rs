use crate::types::*;

/// Parsed output from a single file.
#[derive(Debug, Clone, Default)]
pub struct ParsedItems {
    pub functions: Vec<FnEntry>,
    pub types: Vec<TypeEntry>,
    pub traits: Vec<TraitEntry>,
    pub impls: Vec<ImplEntry>,
}

/// Extract function and type entries from a Verus source file using tree-sitter.
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
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "verus_block" => {
                // verus! { ... } — body field contains a declaration_list
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

    // Trait name from `impl Trait for Type` — the "trait" field
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

    // Extract supertraits from bounds (e.g., `trait Foo: Bar + Baz`)
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

    // Type parameters
    let type_params = node
        .child_by_field_name("type_parameters")
        .map(|n| node_text(&n, source));

    // Parameters
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(&n, source))
        .unwrap_or_else(|| "()".to_string());

    // Return type
    let return_type = node
        .child_by_field_name("return_type")
        .map(|n| node_text(&n, source));

    // Requires and ensures clauses
    let requires = extract_clause(node, source, "requires_clause");
    let ensures = extract_clause(node, source, "ensures_clause");

    // Body text (for function_item, not function_signature_item)
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

    // Extract field declarations
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

    // Extract variant names
    let mut fields = Vec::new();
    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "enum_variant" {
                // Get just the variant name (first named child)
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

    // The aliased type (right-hand side of =)
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

/// Extract predicates from a requires_clause or ensures_clause node.
fn extract_clause(
    fn_node: &tree_sitter::Node,
    source: &str,
    clause_kind: &str,
) -> Vec<String> {
    let mut results = Vec::new();

    let mut cursor = fn_node.walk();
    for child in fn_node.children(&mut cursor) {
        if child.kind() == clause_kind {
            // The clause's children are: keyword, then expression nodes separated by commas
            let mut expr_cursor = child.walk();
            for expr in child.children(&mut expr_cursor) {
                let kind = expr.kind();
                // Skip the keyword itself and comma punctuation
                if kind == "requires" || kind == "ensures" || kind == "," {
                    continue;
                }
                // Skip anonymous nodes that are just punctuation
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

    // Fallback: look at raw source text above the function
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
