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
    pub file_path: String,
    pub line: usize,
}

impl FnEntry {
    /// Format as a compact signature string for search results.
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

    /// Full display with module, file, requires, ensures.
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
            "{prefix} {}{tparams}{}{ret}\n  module: {}\n  file:   {}:{}\n{trait_ctx}{doc}",
            self.name, self.params, self.module_path, self.file_path, self.line,
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
