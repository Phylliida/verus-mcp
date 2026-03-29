#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FnKind {
    Spec,
    Proof,
    Exec,
}

impl FnKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            FnKind::Spec => "spec",
            FnKind::Proof => "proof",
            FnKind::Exec => "exec",
        }
    }
}

impl std::fmt::Display for FnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    Public,
    PublicCrate,
    Private,
}

impl Visibility {
    pub fn as_str(&self) -> &'static str {
        match self {
            Visibility::Public => "pub",
            Visibility::PublicCrate => "pub(crate)",
            Visibility::Private => "",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FnEntry {
    pub name: String,
    pub kind: FnKind,
    pub visibility: Visibility,
    pub is_open: bool,
    pub type_params: Option<String>,
    pub params: String,
    pub return_type: Option<String>,
    pub requires: Vec<String>,
    pub ensures: Vec<String>,
    pub crate_name: String,
    pub module_path: String,
    pub trait_name: Option<String>,
    pub doc_comment: Option<String>,
    pub body: Option<String>,
    pub file_path: String,
    pub line: usize,
    pub end_line: usize,
}

impl FnEntry {
    ///  Format as a compact signature string for search results.
    pub fn format_signature(&self) -> String {
        let vis = self.visibility.as_str();
        let open = if self.is_open { "open " } else { "" };
        let kind = self.kind.as_str();
        let tparams = self.type_params.as_deref().unwrap_or("");
        let ret = self
            .return_type
            .as_ref()
            .map(|r| format!(" -> {}", r))
            .unwrap_or_default();

        let prefix = if vis.is_empty() {
            format!("{open}{kind} fn")
        } else {
            format!("{vis} {open}{kind} fn")
        };

        format!("{prefix} {}{tparams}{}{ret}", self.name, self.params)
    }

    fn filename(&self) -> &str {
        self.file_path
            .rsplit('/')
            .next()
            .unwrap_or(&self.file_path)
    }

    ///  One-line compact format for search result lists.
    ///  `[kind] name  (filename:line)`
    pub fn format_compact(&self) -> String {
        format!("[{}] {}  ({}:{})", self.kind, self.name, self.filename(), self.line)
    }

    ///  Compact format with a matching clause snippet (for ensures/requires search).
    ///  If total clause text is ≤300 chars, shows all clauses for full context.
    ///  Otherwise centers a 120-char window around the match in the matching clause.
    ///  `matcher` returns the byte offset of the match start within a string, or None.
    pub fn format_clause_match(
        &self,
        clauses: &[String],
        matcher: &dyn Fn(&str) -> Option<usize>,
    ) -> String {
        let trimmed: Vec<&str> = clauses.iter().map(|c| c.trim()).collect();
        let total_len: usize = trimmed.iter().map(|c| c.len()).sum();

        let snippet = if total_len <= 300 {
            //  Show all clauses for full context
            trimmed
                .iter()
                .map(|c| format!("    {}", c))
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            //  Fall back to centered snippet of matching clause
            let s = clauses
                .iter()
                .find(|c| matcher(c).is_some())
                .map(|c| Self::snippet_around(c.trim(), matcher))
                .unwrap_or_default();
            format!("    {}", s)
        };
        format!(
            "[{}] {}  ({}:{})\n{}",
            self.kind, self.name, self.filename(), self.line, snippet
        )
    }

    ///  Compact format with a matching body snippet.
    ///  `matcher` returns the byte offset of the match start within a string, or None.
    pub fn format_body_match(&self, matcher: &dyn Fn(&str) -> Option<usize>) -> String {
        let snippet = self
            .body
            .as_ref()
            .and_then(|b| {
                //  Find the line containing the match and show it
                let pos = matcher(b)?;
                let line_start = b[..pos].rfind('\n').map(|i| i + 1).unwrap_or(0);
                let line_end = b[pos..].find('\n').map(|i| pos + i).unwrap_or(b.len());
                let line = b[line_start..line_end].trim();
                Some(Self::snippet_around(line, matcher))
            })
            .unwrap_or_default();
        format!(
            "[{}] {}  ({}:{})\n    {}",
            self.kind, self.name, self.filename(), self.line, snippet
        )
    }

    ///  Extract a centered 120-char window around the match position in `text`.
    fn snippet_around(text: &str, matcher: &dyn Fn(&str) -> Option<usize>) -> String {
        if text.len() <= 120 {
            return text.to_string();
        }
        let match_pos = matcher(text).unwrap_or(0);
        let window = 120usize;
        let half = window / 2;
        let start = match_pos.saturating_sub(half);
        let end = (start + window).min(text.len());
        let start = if end == text.len() {
            end.saturating_sub(window)
        } else {
            start
        };
        let start = text.floor_char_boundary(start);
        let end = text.floor_char_boundary(end);
        let prefix = if start > 0 { "..." } else { "" };
        let suffix = if end < text.len() { "..." } else { "" };
        format!("{}{}{}", prefix, &text[start..end], suffix)
    }

    ///  Full display with module, file, requires, ensures.
    pub fn format_full(&self) -> String {
        let vis = self.visibility.as_str();
        let open = if self.is_open { "open " } else { "" };
        let kind = self.kind.as_str();
        let tparams = self.type_params.as_deref().unwrap_or("");
        let ret = self
            .return_type
            .as_ref()
            .map(|r| format!(" -> {}", r))
            .unwrap_or_default();
        let trait_ctx = self
            .trait_name
            .as_ref()
            .map(|t| format!("  trait:  {}\n", t))
            .unwrap_or_default();
        let doc = self
            .doc_comment
            .as_ref()
            .map(|d| format!("  doc:    {}\n", d))
            .unwrap_or_default();

        let prefix = if vis.is_empty() {
            format!("[{kind}] {open}fn")
        } else {
            format!("[{kind}] {vis} {open}fn")
        };

        let mut out = format!(
            "{prefix} {}{tparams}{}{ret}\n  module: {}\n  file:   {}:{}-{}\n{trait_ctx}{doc}",
            self.name, self.params, self.module_path, self.file_path, self.line, self.end_line,
        );

        if !self.requires.is_empty() {
            out.push_str("  requires:\n");
            for r in &self.requires {
                out.push_str(&format!("    {}\n", r));
            }
        }
        if !self.ensures.is_empty() {
            out.push_str("  ensures:\n");
            for e in &self.ensures {
                out.push_str(&format!("    {}\n", e));
            }
        }

        out
    }
}

//  --- Type entries (structs, enums, type aliases) ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Struct,
    Enum,
    TypeAlias,
}

impl ItemKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ItemKind::Struct => "struct",
            ItemKind::Enum => "enum",
            ItemKind::TypeAlias => "type",
        }
    }
}

impl std::fmt::Display for ItemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct TypeEntry {
    pub name: String,
    pub item_kind: ItemKind,
    pub visibility: Visibility,
    pub type_params: Option<String>,
    ///  Field declarations (struct) or variant names (enum).
    pub fields: Vec<String>,
    ///  The aliased type (for type aliases only).
    pub aliased_type: Option<String>,
    pub crate_name: String,
    pub module_path: String,
    pub doc_comment: Option<String>,
    pub file_path: String,
    pub line: usize,
    pub end_line: usize,
}

impl TypeEntry {
    fn filename(&self) -> &str {
        self.file_path
            .rsplit('/')
            .next()
            .unwrap_or(&self.file_path)
    }

    ///  One-line compact format: `[struct] Vec2<T: Ring>  (vec2.rs:5)`
    pub fn format_compact(&self) -> String {
        let tparams = self.type_params.as_deref().unwrap_or("");
        format!(
            "[{}] {}{}  ({}:{})",
            self.item_kind,
            self.name,
            tparams,
            self.filename(),
            self.line
        )
    }

    ///  Full display with module, fields/variants.
    pub fn format_full(&self) -> String {
        let vis = self.visibility.as_str();
        let tparams = self.type_params.as_deref().unwrap_or("");
        let doc = self
            .doc_comment
            .as_ref()
            .map(|d| format!("  doc:    {}\n", d))
            .unwrap_or_default();

        let prefix = if vis.is_empty() {
            format!("[{}]", self.item_kind)
        } else {
            format!("[{}] {}", self.item_kind, vis)
        };

        let mut out = format!(
            "{} {}{}\n  module: {}\n  file:   {}:{}-{}\n{}",
            prefix, self.name, tparams, self.module_path, self.file_path, self.line, self.end_line, doc
        );

        if let Some(ref aliased) = self.aliased_type {
            out.push_str(&format!("  = {}\n", aliased));
        }

        if !self.fields.is_empty() {
            let label = match self.item_kind {
                ItemKind::Enum => "  variants:\n",
                _ => "  fields:\n",
            };
            out.push_str(label);
            for f in &self.fields {
                out.push_str(&format!("    {}\n", f));
            }
        }

        out
    }
}

//  --- Trait and impl entries ---

#[derive(Debug, Clone)]
pub struct TraitEntry {
    pub name: String,
    pub visibility: Visibility,
    pub type_params: Option<String>,
    pub supertraits: Option<String>,
    pub method_names: Vec<String>,
    pub crate_name: String,
    pub module_path: String,
    pub doc_comment: Option<String>,
    pub file_path: String,
    pub line: usize,
    pub end_line: usize,
}

impl TraitEntry {
    fn filename(&self) -> &str {
        self.file_path
            .rsplit('/')
            .next()
            .unwrap_or(&self.file_path)
    }

    pub fn format_compact(&self) -> String {
        let tparams = self.type_params.as_deref().unwrap_or("");
        let supers = self
            .supertraits
            .as_ref()
            .map(|s| format!(": {}", s))
            .unwrap_or_default();
        format!(
            "[trait] {}{}{}  ({}:{})",
            self.name,
            tparams,
            supers,
            self.filename(),
            self.line
        )
    }

    pub fn format_full(&self) -> String {
        let vis = self.visibility.as_str();
        let tparams = self.type_params.as_deref().unwrap_or("");
        let supers = self
            .supertraits
            .as_ref()
            .map(|s| format!(": {}", s))
            .unwrap_or_default();
        let doc = self
            .doc_comment
            .as_ref()
            .map(|d| format!("  doc:     {}\n", d))
            .unwrap_or_default();

        let prefix = if vis.is_empty() {
            "[trait]".to_string()
        } else {
            format!("[trait] {}", vis)
        };

        let mut out = format!(
            "{} {}{}{}\n  module:  {}\n  file:    {}:{}-{}\n{}",
            prefix, self.name, tparams, supers, self.module_path, self.file_path, self.line, self.end_line, doc
        );

        if !self.method_names.is_empty() {
            out.push_str(&format!("  methods: {}\n", self.method_names.join(", ")));
        }

        out
    }
}

#[derive(Debug, Clone)]
pub struct ImplEntry {
    ///  None for inherent impls.
    pub trait_name: Option<String>,
    pub type_name: String,
    pub type_params: Option<String>,
    pub method_names: Vec<String>,
    pub crate_name: String,
    pub module_path: String,
    pub file_path: String,
    pub line: usize,
    pub end_line: usize,
}

impl ImplEntry {
    fn filename(&self) -> &str {
        self.file_path
            .rsplit('/')
            .next()
            .unwrap_or(&self.file_path)
    }

    pub fn format_compact(&self) -> String {
        let tparams = self.type_params.as_deref().unwrap_or("");
        if let Some(ref trait_name) = self.trait_name {
            format!(
                "[impl] {}{} for {}  ({}:{})",
                trait_name,
                tparams,
                self.type_name,
                self.filename(),
                self.line
            )
        } else {
            format!(
                "[impl] {}{}  ({}:{})",
                self.type_name,
                tparams,
                self.filename(),
                self.line
            )
        }
    }

    pub fn format_full(&self) -> String {
        let tparams = self.type_params.as_deref().unwrap_or("");
        let header = if let Some(ref trait_name) = self.trait_name {
            format!("[impl] {}{} for {}", trait_name, tparams, self.type_name)
        } else {
            format!("[impl] {}{}", self.type_name, tparams)
        };

        let mut out = format!(
            "{}\n  module:  {}\n  file:    {}:{}-{}\n",
            header, self.module_path, self.file_path, self.line, self.end_line
        );

        if !self.method_names.is_empty() {
            out.push_str(&format!("  methods: {}\n", self.method_names.join(", ")));
        }

        out
    }
}
