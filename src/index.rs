use crate::indexer::FileCache;
use crate::types::{FnEntry, FnKind};

const MAX_RESULTS: usize = 50;

pub struct Index {
    entries: Vec<FnEntry>,
    file_cache: FileCache,
}

impl Index {
    pub fn new(entries: Vec<FnEntry>, file_cache: FileCache) -> Self {
        Self {
            entries,
            file_cache,
        }
    }

    pub fn file_cache(&self) -> &FileCache {
        &self.file_cache
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Search by name substring, with optional filters.
    pub fn search(
        &self,
        query: &str,
        kind: Option<FnKind>,
        crate_name: Option<&str>,
        module: Option<&str>,
        trait_only: bool,
    ) -> Vec<&FnEntry> {
        let q = query.to_lowercase();
        self.entries
            .iter()
            .filter(|e| e.name.to_lowercase().contains(&q))
            .filter(|e| kind.map_or(true, |k| e.kind == k))
            .filter(|e| crate_name.map_or(true, |c| e.crate_name.eq_ignore_ascii_case(c)))
            .filter(|e| {
                module.map_or(true, |m| {
                    e.module_path.to_lowercase().contains(&m.to_lowercase())
                })
            })
            .filter(|e| !trait_only || e.trait_name.is_some())
            .take(MAX_RESULTS)
            .collect()
    }

    /// Exact name match.
    pub fn lookup(&self, name: &str) -> Vec<&FnEntry> {
        self.entries
            .iter()
            .filter(|e| e.name.eq_ignore_ascii_case(name))
            .take(MAX_RESULTS)
            .collect()
    }

    /// Search within ensures clauses.
    pub fn search_ensures(&self, query: &str) -> Vec<&FnEntry> {
        let q = query.to_lowercase();
        self.entries
            .iter()
            .filter(|e| {
                e.ensures
                    .iter()
                    .any(|clause| clause.to_lowercase().contains(&q))
            })
            .take(MAX_RESULTS)
            .collect()
    }

    /// Search within requires clauses.
    pub fn search_requires(&self, query: &str) -> Vec<&FnEntry> {
        let q = query.to_lowercase();
        self.entries
            .iter()
            .filter(|e| {
                e.requires
                    .iter()
                    .any(|clause| clause.to_lowercase().contains(&q))
            })
            .take(MAX_RESULTS)
            .collect()
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
    ) -> Vec<&FnEntry> {
        self.entries
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
            .filter(|e| crate_name.map_or(true, |c| e.crate_name.eq_ignore_ascii_case(c)))
            .filter(|e| {
                module.map_or(true, |m| {
                    e.module_path.to_lowercase().contains(&m.to_lowercase())
                })
            })
            .take(MAX_RESULTS)
            .collect()
    }

    /// List all unique modules with item counts.
    pub fn list_modules(&self) -> Vec<(String, usize)> {
        let mut map = std::collections::BTreeMap::<String, usize>::new();
        for e in &self.entries {
            *map.entry(e.module_path.clone()).or_default() += 1;
        }
        map.into_iter().collect()
    }
}
