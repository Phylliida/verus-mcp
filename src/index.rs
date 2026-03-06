use crate::indexer::FileCache;
use crate::types::{FnEntry, FnKind, ImplEntry, TraitEntry, TypeEntry};
use regex::RegexBuilder;
use std::collections::{HashMap, HashSet};

pub const MAX_RESULTS: usize = 50;
pub const DEFAULT_RESULTS: usize = 4;

/// Parse a potentially qualified name like `crate::vec2::ops::foo` into (module_prefix, name).
/// Returns `None` for the module prefix when the name is unqualified.
/// Strips leading `crate::` or `crate_name::` prefixes.
fn parse_qualified_name(input: &str) -> (Option<&str>, &str) {
    if let Some(pos) = input.rfind("::") {
        let module_part = &input[..pos];
        let name_part = &input[pos + 2..];
        // Strip leading "crate::" or bare "crate" (crate root)
        let module_part = module_part
            .strip_prefix("crate::")
            .unwrap_or(module_part);
        if module_part == "crate" {
            // crate::foo → crate-root item, match by name only against empty module_path
            (Some(""), name_part)
        } else {
            (Some(module_part), name_part)
        }
    } else {
        (None, input)
    }
}

/// Strip generic parameters from a module path.
/// e.g., "limits::Limits<T>" → "limits::Limits"
fn strip_generics(path: &str) -> &str {
    match path.find('<') {
        Some(pos) => &path[..pos],
        None => path,
    }
}

/// Check if a module_path matches the given qualifier.
/// The qualifier might be a suffix of the full module path (e.g., "vec2::ops" matches "vec2::ops")
/// or it might include a crate name prefix that we should match against crate_name::module_path.
/// Generic parameters are stripped before matching (e.g., qualifier "Limits" matches "limits::Limits<T>").
fn module_matches(module_path: &str, crate_name: &str, qualifier: &str) -> bool {
    let module_path_clean = strip_generics(module_path);
    let qualifier_clean = strip_generics(qualifier);
    // Direct match
    if module_path_clean.eq_ignore_ascii_case(qualifier_clean) {
        return true;
    }
    // Qualifier might be crate_name::module_path
    let full_path = format!("{}::{}", crate_name, module_path_clean);
    if full_path.eq_ignore_ascii_case(qualifier_clean) {
        return true;
    }
    // Qualifier might be a suffix (e.g., "ops" matching "vec2::ops", or "Limits" matching "limits::Limits")
    if module_path_clean.ends_with(qualifier_clean)
        && (module_path_clean.len() == qualifier_clean.len()
            || module_path_clean.as_bytes()[module_path_clean.len() - qualifier_clean.len() - 1] == b':')
    {
        return true;
    }
    false
}

fn short_filename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

pub struct Suggestion {
    pub name: String,
    pub label: String,
    pub module_path: String,
    pub location: String,
    pub score: f64,
}

/// A compiled matcher: regex if the query parses as one, otherwise plain substring.
pub enum Matcher {
    Regex(regex::Regex),
    Substring(String),
}

impl Matcher {
    /// Try to compile as case-insensitive regex; fall back to lowercase substring.
    pub fn new(query: &str) -> Self {
        match RegexBuilder::new(query).case_insensitive(true).build() {
            Ok(re) => Matcher::Regex(re),
            Err(_) => Matcher::Substring(query.to_lowercase()),
        }
    }

    fn is_match(&self, text: &str) -> bool {
        match self {
            Matcher::Regex(re) => re.is_match(text),
            Matcher::Substring(q) => text.to_lowercase().contains(q),
        }
    }

    /// Return the byte offset of the first match, or None.
    pub fn find_pos(&self, text: &str) -> Option<usize> {
        match self {
            Matcher::Regex(re) => re.find(text).map(|m| m.start()),
            Matcher::Substring(q) => text.to_lowercase().find(q),
        }
    }
}

#[derive(Default)]
pub struct CrateStats {
    pub functions: usize,
    pub types: usize,
    pub traits: usize,
    pub assume_false: usize,
}

pub struct IndexStats {
    pub total_functions: usize,
    pub total_types: usize,
    pub total_traits: usize,
    pub spec: usize,
    pub proof: usize,
    pub exec: usize,
    pub assume_false: usize,
    pub by_crate: std::collections::BTreeMap<String, CrateStats>,
}

/// Normalize crate name: replace hyphens with underscores and lowercase.
fn normalize_crate(name: &str) -> String {
    name.to_lowercase().replace('-', "_")
}

/// Compare two crate names, treating hyphens and underscores as equivalent.
fn crate_name_matches(entry_crate: &str, filter_crate: &str) -> bool {
    normalize_crate(entry_crate) == normalize_crate(filter_crate)
}

pub struct SearchResult<'a> {
    pub items: Vec<&'a FnEntry>,
    pub total_count: usize,
}

pub struct TypeSearchResult<'a> {
    pub items: Vec<&'a TypeEntry>,
    pub total_count: usize,
}

pub struct Index {
    entries: Vec<FnEntry>,
    type_entries: Vec<TypeEntry>,
    trait_entries: Vec<TraitEntry>,
    impl_entries: Vec<ImplEntry>,
    /// For each function name, indices of entries that call it.
    callers: HashMap<String, Vec<usize>>,
    /// For each entry index, set of function names it calls.
    callees: Vec<HashSet<String>>,
    file_cache: FileCache,
}

/// Tokenize body text into identifier-like tokens.
fn tokenize_body(body: &str) -> HashSet<&str> {
    body.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
        .collect()
}

/// Apply offset + limit pagination to a collected Vec.
fn paginate<T>(items: Vec<T>, offset: usize, limit: usize) -> Vec<T> {
    items.into_iter().skip(offset).take(limit).collect()
}

impl Index {
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            type_entries: Vec::new(),
            trait_entries: Vec::new(),
            impl_entries: Vec::new(),
            callers: HashMap::new(),
            callees: Vec::new(),
            file_cache: FileCache::new(),
        }
    }

    pub fn new(
        entries: Vec<FnEntry>,
        type_entries: Vec<TypeEntry>,
        trait_entries: Vec<TraitEntry>,
        impl_entries: Vec<ImplEntry>,
        file_cache: FileCache,
    ) -> Self {
        // Build known function name set
        let known_names: HashSet<String> = entries.iter().map(|e| e.name.clone()).collect();

        // Build call graph
        let mut callers: HashMap<String, Vec<usize>> = HashMap::new();
        let mut callees: Vec<HashSet<String>> = Vec::with_capacity(entries.len());

        for (idx, entry) in entries.iter().enumerate() {
            let mut entry_callees = HashSet::new();
            if let Some(ref body) = entry.body {
                let tokens = tokenize_body(body);
                for token in tokens {
                    if known_names.contains(token) && token != entry.name {
                        entry_callees.insert(token.to_string());
                        callers.entry(token.to_string()).or_default().push(idx);
                    }
                }
            }
            callees.push(entry_callees);
        }

        Self {
            entries,
            type_entries,
            trait_entries,
            impl_entries,
            callers,
            callees,
            file_cache,
        }
    }

    pub fn file_cache(&self) -> &FileCache {
        &self.file_cache
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn type_len(&self) -> usize {
        self.type_entries.len()
    }

    pub fn trait_len(&self) -> usize {
        self.trait_entries.len()
    }

    pub fn impl_len(&self) -> usize {
        self.impl_entries.len()
    }

    /// Search by name substring, with optional filters.
    /// Results ranked: exact match > prefix > substring, then by name length ascending.
    pub fn search(
        &self,
        query: &str,
        kind: Option<FnKind>,
        crate_name: Option<&str>,
        module: Option<&str>,
        trait_only: bool,
        offset: usize,
        limit: usize,
    ) -> SearchResult<'_> {
        let q = query.to_lowercase();
        let mut matches: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| e.name.to_lowercase().contains(&q))
            .filter(|e| kind.map_or(true, |k| e.kind == k))
            .filter(|e| crate_name.map_or(true, |c| crate_name_matches(&e.crate_name, c)))
            .filter(|e| {
                module.map_or(true, |m| {
                    e.module_path.to_lowercase().contains(&m.to_lowercase())
                })
            })
            .filter(|e| !trait_only || e.trait_name.is_some())
            .collect();

        let total_count = matches.len();

        // Rank: exact (0) > prefix (1) > substring (2), then by name length
        matches.sort_by(|a, b| {
            let a_lower = a.name.to_lowercase();
            let b_lower = b.name.to_lowercase();
            let a_tier = if a_lower == q {
                0
            } else if a_lower.starts_with(&q) {
                1
            } else {
                2
            };
            let b_tier = if b_lower == q {
                0
            } else if b_lower.starts_with(&q) {
                1
            } else {
                2
            };
            a_tier.cmp(&b_tier).then(a.name.len().cmp(&b.name.len()))
        });

        let items = paginate(matches, offset, limit);

        SearchResult {
            items,
            total_count,
        }
    }

    /// Exact name match for functions.
    pub fn lookup(&self, name: &str) -> Vec<&FnEntry> {
        let (qualifier, bare_name) = parse_qualified_name(name);
        self.entries
            .iter()
            .filter(|e| {
                e.name.eq_ignore_ascii_case(bare_name)
                    && qualifier.map_or(true, |q| {
                        module_matches(&e.module_path, &e.crate_name, q)
                    })
            })
            .take(MAX_RESULTS)
            .collect()
    }

    /// Exact name match for types (fallback when fn lookup finds nothing).
    pub fn lookup_type(&self, name: &str) -> Vec<&TypeEntry> {
        let (qualifier, bare_name) = parse_qualified_name(name);
        self.type_entries
            .iter()
            .filter(|e| {
                e.name.eq_ignore_ascii_case(bare_name)
                    && qualifier.map_or(true, |q| {
                        module_matches(&e.module_path, &e.crate_name, q)
                    })
            })
            .take(MAX_RESULTS)
            .collect()
    }

    /// Exact name match for traits.
    pub fn lookup_trait(&self, name: &str) -> Vec<&TraitEntry> {
        let (qualifier, bare_name) = parse_qualified_name(name);
        self.trait_entries
            .iter()
            .filter(|e| {
                e.name.eq_ignore_ascii_case(bare_name)
                    && qualifier.map_or(true, |q| {
                        module_matches(&e.module_path, &e.crate_name, q)
                    })
            })
            .take(MAX_RESULTS)
            .collect()
    }

    /// Filter helper for optional crate_name, module, name, and kind filters on FnEntry.
    fn matches_fn_filters(
        e: &FnEntry,
        crate_name: Option<&str>,
        module: Option<&str>,
        name: Option<&str>,
        kind: Option<FnKind>,
    ) -> bool {
        if let Some(c) = crate_name {
            if !crate_name_matches(&e.crate_name, c) {
                return false;
            }
        }
        if let Some(m) = module {
            if !e.module_path.to_lowercase().contains(&m.to_lowercase()) {
                return false;
            }
        }
        if let Some(n) = name {
            if !e.name.to_lowercase().contains(&n.to_lowercase()) {
                return false;
            }
        }
        if let Some(k) = kind {
            if e.kind != k {
                return false;
            }
        }
        true
    }

    /// Search within ensures clauses. Query supports regex (falls back to substring).
    pub fn search_ensures(
        &self,
        query: &str,
        crate_name: Option<&str>,
        module: Option<&str>,
        name: Option<&str>,
        kind: Option<FnKind>,
        offset: usize,
        limit: usize,
    ) -> SearchResult<'_> {
        let m = Matcher::new(query);
        let matches: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| Self::matches_fn_filters(e, crate_name, module, name, kind))
            .filter(|e| {
                e.ensures
                    .iter()
                    .any(|clause| m.is_match(clause))
            })
            .collect();

        let total_count = matches.len();
        let items = paginate(matches, offset, limit);

        SearchResult { items, total_count }
    }

    /// Search within requires clauses. Query supports regex (falls back to substring).
    pub fn search_requires(
        &self,
        query: &str,
        crate_name: Option<&str>,
        module: Option<&str>,
        name: Option<&str>,
        kind: Option<FnKind>,
        offset: usize,
        limit: usize,
    ) -> SearchResult<'_> {
        let m = Matcher::new(query);
        let matches: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| Self::matches_fn_filters(e, crate_name, module, name, kind))
            .filter(|e| {
                e.requires
                    .iter()
                    .any(|clause| m.is_match(clause))
            })
            .collect();

        let total_count = matches.len();
        let items = paginate(matches, offset, limit);

        SearchResult { items, total_count }
    }

    /// Search within function bodies for usage of a lemma or pattern. Query supports regex (falls back to substring).
    pub fn search_body(
        &self,
        query: &str,
        crate_name: Option<&str>,
        module: Option<&str>,
        name: Option<&str>,
        kind: Option<FnKind>,
        offset: usize,
        limit: usize,
    ) -> SearchResult<'_> {
        let m = Matcher::new(query);
        let matches: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| Self::matches_fn_filters(e, crate_name, module, name, kind))
            .filter(|e| {
                e.body
                    .as_ref()
                    .map_or(false, |b| m.is_match(b))
            })
            .collect();

        let total_count = matches.len();
        let items = paginate(matches, offset, limit);

        SearchResult { items, total_count }
    }

    /// Search within doc comments of functions. Query supports regex (falls back to substring).
    pub fn search_doc(
        &self,
        query: &str,
        crate_name: Option<&str>,
        module: Option<&str>,
        name: Option<&str>,
        kind: Option<FnKind>,
        offset: usize,
        limit: usize,
    ) -> SearchResult<'_> {
        let m = Matcher::new(query);
        let matches: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| Self::matches_fn_filters(e, crate_name, module, name, kind))
            .filter(|e| {
                e.doc_comment
                    .as_ref()
                    .map_or(false, |d| m.is_match(d))
            })
            .collect();

        let total_count = matches.len();
        let items = paginate(matches, offset, limit);

        SearchResult { items, total_count }
    }

    /// Search within doc comments of types. Query supports regex (falls back to substring).
    pub fn search_type_doc(
        &self,
        query: &str,
        crate_name: Option<&str>,
        module: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> TypeSearchResult<'_> {
        let m = Matcher::new(query);
        let matches: Vec<&TypeEntry> = self
            .type_entries
            .iter()
            .filter(|e| {
                if let Some(c) = crate_name {
                    if !crate_name_matches(&e.crate_name, c) {
                        return false;
                    }
                }
                if let Some(mo) = module {
                    if !e.module_path.to_lowercase().contains(&mo.to_lowercase()) {
                        return false;
                    }
                }
                true
            })
            .filter(|e| {
                e.doc_comment
                    .as_ref()
                    .map_or(false, |d| m.is_match(d))
            })
            .collect();

        let total_count = matches.len();
        let items = paginate(matches, offset, limit);

        TypeSearchResult { items, total_count }
    }

    /// Search by signature types and trait bounds.
    /// At least one of param_type, return_type, or type_bound must be provided.
    pub fn search_signature(
        &self,
        param_type: Option<&str>,
        return_type: Option<&str>,
        type_bound: Option<&str>,
        name: Option<&str>,
        kind: Option<FnKind>,
        crate_name: Option<&str>,
        module: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> SearchResult<'_> {
        let mut matches: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| {
                param_type.map_or(true, |q| {
                    e.params.to_lowercase().contains(&q.to_lowercase())
                })
            })
            .filter(|e| {
                return_type.map_or(true, |q| {
                    e.return_type
                        .as_ref()
                        .map_or(false, |r| r.to_lowercase().contains(&q.to_lowercase()))
                })
            })
            .filter(|e| {
                type_bound.map_or(true, |q| {
                    e.type_params
                        .as_ref()
                        .map_or(false, |tp| tp.to_lowercase().contains(&q.to_lowercase()))
                })
            })
            .filter(|e| {
                name.map_or(true, |q| {
                    e.name.to_lowercase().contains(&q.to_lowercase())
                })
            })
            .filter(|e| kind.map_or(true, |k| e.kind == k))
            .filter(|e| crate_name.map_or(true, |c| crate_name_matches(&e.crate_name, c)))
            .filter(|e| {
                module.map_or(true, |m| {
                    e.module_path.to_lowercase().contains(&m.to_lowercase())
                })
            })
            .collect();

        let total_count = matches.len();

        // Rank by name when name filter is provided
        if let Some(n) = name {
            let n_lower = n.to_lowercase();
            matches.sort_by(|a, b| {
                let a_lower = a.name.to_lowercase();
                let b_lower = b.name.to_lowercase();
                let a_tier = if a_lower == n_lower {
                    0
                } else if a_lower.starts_with(&n_lower) {
                    1
                } else {
                    2
                };
                let b_tier = if b_lower == n_lower {
                    0
                } else if b_lower.starts_with(&n_lower) {
                    1
                } else {
                    2
                };
                a_tier.cmp(&b_tier).then(a.name.len().cmp(&b.name.len()))
            });
        }

        let items = paginate(matches, offset, limit);

        SearchResult {
            items,
            total_count,
        }
    }

    /// Search types (structs, enums, type aliases) by name substring.
    pub fn search_types(
        &self,
        query: &str,
        crate_name: Option<&str>,
        module: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> TypeSearchResult<'_> {
        let q = query.to_lowercase();
        let mut matches: Vec<&TypeEntry> = self
            .type_entries
            .iter()
            .filter(|e| {
                if let Some(c) = crate_name {
                    if !crate_name_matches(&e.crate_name, c) {
                        return false;
                    }
                }
                if let Some(m) = module {
                    if !e.module_path.to_lowercase().contains(&m.to_lowercase()) {
                        return false;
                    }
                }
                true
            })
            .filter(|e| e.name.to_lowercase().contains(&q))
            .collect();

        let total_count = matches.len();

        // Rank: exact > prefix > substring, then by name length
        matches.sort_by(|a, b| {
            let a_lower = a.name.to_lowercase();
            let b_lower = b.name.to_lowercase();
            let a_tier = if a_lower == q {
                0
            } else if a_lower.starts_with(&q) {
                1
            } else {
                2
            };
            let b_tier = if b_lower == q {
                0
            } else if b_lower.starts_with(&q) {
                1
            } else {
                2
            };
            a_tier.cmp(&b_tier).then(a.name.len().cmp(&b.name.len()))
        });

        let items = paginate(matches, offset, limit);

        TypeSearchResult {
            items,
            total_count,
        }
    }

    /// Search trait impls by trait name substring.
    pub fn search_trait_impls(&self, trait_name: &str) -> Vec<&ImplEntry> {
        let q = trait_name.to_lowercase();
        self.impl_entries
            .iter()
            .filter(|e| {
                e.trait_name
                    .as_ref()
                    .map_or(false, |t| t.to_lowercase().contains(&q))
            })
            .collect()
    }

    /// Fuzzy search by name using Jaro-Winkler similarity with 0.75 threshold.
    pub fn search_fuzzy(&self, query: &str, limit: usize) -> SearchResult<'_> {
        let q = query.to_lowercase();
        let threshold = 0.75;

        let mut scored: Vec<(&FnEntry, f64)> = self
            .entries
            .iter()
            .map(|e| {
                let score = strsim::jaro_winkler(&e.name.to_lowercase(), &q);
                (e, score)
            })
            .filter(|(_, score)| *score >= threshold)
            .collect();

        let total_count = scored.len();

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        SearchResult {
            items: scored.into_iter().map(|(e, _)| e).collect(),
            total_count,
        }
    }

    /// Compute similarity between a name and query (both already lowercased).
    /// Substring containment scores 0.9, otherwise Jaro-Winkler.
    fn similarity_score(name: &str, query: &str) -> f64 {
        if name == query {
            1.0
        } else if name.contains(query) || query.contains(name) {
            0.9
        } else {
            strsim::jaro_winkler(name, query)
        }
    }

    /// Suggest similar names across all indexed items (functions, types, traits).
    /// Used for "Did you mean?" when a search returns no results.
    pub fn suggest(&self, query: &str, limit: usize) -> Vec<Suggestion> {
        let q = query.to_lowercase();
        let threshold = 0.7;

        let mut suggestions: Vec<Suggestion> = Vec::new();

        for e in &self.entries {
            let score = Self::similarity_score(&e.name.to_lowercase(), &q);
            if score >= threshold {
                suggestions.push(Suggestion {
                    name: e.name.clone(),
                    label: format!("[{}]", e.kind),
                    module_path: e.module_path.clone(),
                    location: format!("{}:{}", short_filename(&e.file_path), e.line),
                    score,
                });
            }
        }

        for e in &self.type_entries {
            let score = Self::similarity_score(&e.name.to_lowercase(), &q);
            if score >= threshold {
                suggestions.push(Suggestion {
                    name: e.name.clone(),
                    label: format!("[{}]", e.item_kind),
                    module_path: e.module_path.clone(),
                    location: format!("{}:{}", short_filename(&e.file_path), e.line),
                    score,
                });
            }
        }

        for e in &self.trait_entries {
            let score = Self::similarity_score(&e.name.to_lowercase(), &q);
            if score >= threshold {
                suggestions.push(Suggestion {
                    name: e.name.clone(),
                    label: "[trait]".to_string(),
                    module_path: e.module_path.clone(),
                    location: format!("{}:{}", short_filename(&e.file_path), e.line),
                    score,
                });
            }
        }

        suggestions.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut seen = HashSet::new();
        suggestions.retain(|s| seen.insert(s.name.clone()));
        suggestions.truncate(limit);
        suggestions
    }

    /// Browse a module: returns all functions and types whose module_path matches
    /// (exact or prefix match). Also supports crate-qualified paths like
    /// "verus_topology" or "verus_topology::mesh".
    pub fn browse_module(&self, path: &str) -> (Vec<&FnEntry>, Vec<&TypeEntry>) {
        let p = path.to_lowercase();

        // Check if path starts with a crate name (possibly after stripping "crate::")
        let stripped = p.strip_prefix("crate::").unwrap_or(&p);
        let (crate_filter, mod_filter) = self.parse_browse_path(stripped);

        let fns: Vec<&FnEntry> = self
            .entries
            .iter()
            .filter(|e| self.browse_matches(&e.crate_name, &e.module_path, &p, &crate_filter, &mod_filter))
            .collect();
        let types: Vec<&TypeEntry> = self
            .type_entries
            .iter()
            .filter(|e| self.browse_matches(&e.crate_name, &e.module_path, &p, &crate_filter, &mod_filter))
            .collect();
        (fns, types)
    }

    /// Parse a browse path into (crate_filter, module_filter).
    /// If the path matches a known crate name, split it; otherwise return (None, full_path).
    fn parse_browse_path(&self, path: &str) -> (Option<String>, Option<String>) {
        // Collect known crate names
        let crate_names: HashSet<String> = self.entries.iter()
            .map(|e| normalize_crate(&e.crate_name))
            .chain(self.type_entries.iter().map(|e| normalize_crate(&e.crate_name)))
            .collect();

        // Check if path is exactly a crate name
        if crate_names.contains(&normalize_crate(path)) {
            return (Some(normalize_crate(path)), None);
        }

        // Check if path starts with "crate_name::"
        if let Some(pos) = path.find("::") {
            let prefix = &path[..pos];
            if crate_names.contains(&normalize_crate(prefix)) {
                let remainder = &path[pos + 2..];
                return (Some(normalize_crate(prefix)), Some(remainder.to_string()));
            }
        }

        (None, None)
    }

    /// Check if an entry matches the browse query.
    fn browse_matches(
        &self,
        entry_crate: &str,
        entry_module: &str,
        raw_path: &str,
        crate_filter: &Option<String>,
        mod_filter: &Option<String>,
    ) -> bool {
        // If we identified a crate filter, use it
        if let Some(ref cf) = crate_filter {
            if normalize_crate(entry_crate) != *cf {
                return false;
            }
            return match mod_filter {
                None => true, // browse entire crate
                Some(mf) => {
                    let mp = entry_module.to_lowercase();
                    mp == *mf || mp.starts_with(&format!("{}::", mf))
                }
            };
        }

        // Fallback: original module_path matching
        let mp = entry_module.to_lowercase();
        mp == *raw_path || mp.starts_with(&format!("{}::", raw_path))
    }

    /// Find all functions that call a given function name.
    pub fn find_callers(&self, name: &str) -> Vec<&FnEntry> {
        self.callers
            .get(name)
            .map(|indices| {
                indices
                    .iter()
                    .filter_map(|&i| self.entries.get(i))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find all function names called by a given function name.
    pub fn find_callees(&self, name: &str) -> Vec<&str> {
        // Find the entry index for this name
        self.entries
            .iter()
            .enumerate()
            .find(|(_, e)| e.name == name)
            .and_then(|(idx, _)| self.callees.get(idx))
            .map(|set| set.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Compute stats: counts by kind, by crate, assume(false) count.
    pub fn stats(&self) -> IndexStats {
        let mut spec = 0usize;
        let mut proof = 0usize;
        let mut exec = 0usize;
        let mut assume_false = 0usize;
        let mut by_crate: std::collections::BTreeMap<String, CrateStats> =
            std::collections::BTreeMap::new();

        let assume_re = RegexBuilder::new(r"assume\s*\(\s*false\s*\)")
            .case_insensitive(false)
            .build()
            .unwrap();

        for e in &self.entries {
            match e.kind {
                FnKind::Spec => spec += 1,
                FnKind::Proof => proof += 1,
                FnKind::Exec => exec += 1,
            }
            let cs = by_crate.entry(e.crate_name.clone()).or_default();
            cs.functions += 1;
            if e.body.as_ref().map_or(false, |b| assume_re.is_match(b)) {
                assume_false += 1;
                cs.assume_false += 1;
            }
        }
        for e in &self.type_entries {
            by_crate.entry(e.crate_name.clone()).or_default().types += 1;
        }
        for e in &self.trait_entries {
            by_crate.entry(e.crate_name.clone()).or_default().traits += 1;
        }

        IndexStats {
            total_functions: self.entries.len(),
            total_types: self.type_entries.len(),
            total_traits: self.trait_entries.len(),
            spec,
            proof,
            exec,
            assume_false,
            by_crate,
        }
    }

    /// List all unique modules with item counts (functions + types).
    pub fn list_modules(&self) -> Vec<(String, usize)> {
        let mut map = std::collections::BTreeMap::<String, usize>::new();
        for e in &self.entries {
            *map.entry(e.module_path.clone()).or_default() += 1;
        }
        for e in &self.type_entries {
            *map.entry(e.module_path.clone()).or_default() += 1;
        }
        map.into_iter().collect()
    }
}
