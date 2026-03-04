use crate::parser::{self, ParsedItems};
use crate::types::{FnEntry, ImplEntry, TraitEntry, TypeEntry};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use walkdir::WalkDir;

/// Per-file cache: maps absolute path → (mtime, parsed items).
pub type FileCache = HashMap<PathBuf, (SystemTime, ParsedItems)>;

/// Default crate source roots relative to the binary's working directory.
const DEFAULT_ROOTS: &[(&str, &str)] = &[
    ("verus-algebra", "verus-algebra/src"),
    ("verus-linalg", "verus-linalg/src"),
    ("verus-geometry", "verus-geometry/src"),
    ("verus-bigint", "verus-bigint/src"),
    ("verus-rational", "verus-rational/src"),
    ("verus-interval-arithmetic", "verus-interval-arithmetic/src"),
    ("verus-topology", "verus-topology/src"),
];

/// Discover all .rs files under a directory.
fn collect_rs_files(dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && e.path().extension().map_or(false, |ext| ext == "rs")
        })
        .map(|e| e.into_path())
        .collect()
}

/// Derive module path from a file's relative path within its crate src dir.
fn module_path_from_rel(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".rs")
        .replace('/', "::")
        .replace("mod::", "")
        .replace("::mod", "")
        .replace("lib", "crate")
}

/// Parse crate roots from VERUS_MCP_ROOTS env var.
/// Format: "crate_name=path,crate_name=path,..."
fn roots_from_env() -> Option<Vec<(String, PathBuf)>> {
    let val = std::env::var("VERUS_MCP_ROOTS").ok()?;
    let mut roots = Vec::new();
    for entry in val.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        if let Some((name, path)) = entry.split_once('=') {
            roots.push((name.trim().to_string(), PathBuf::from(path.trim())));
        }
    }
    if roots.is_empty() {
        None
    } else {
        Some(roots)
    }
}

/// Resolve the base directory — walk up from cwd looking for a directory
/// that contains the expected crate dirs.
fn find_workspace_root() -> PathBuf {
    if let Ok(val) = std::env::var("VERUS_MCP_WORKSPACE") {
        return PathBuf::from(val);
    }
    let mut dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    loop {
        // Check if this dir contains verus-algebra (a good marker)
        if dir.join("verus-algebra").is_dir() {
            return dir;
        }
        if !dir.pop() {
            break;
        }
    }
    // Fallback to cwd
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Build the full index from scratch (empty cache).
pub fn build_index() -> (Vec<FnEntry>, Vec<TypeEntry>, Vec<TraitEntry>, Vec<ImplEntry>, FileCache) {
    build_index_incremental(&FileCache::new())
}

/// Incrementally rebuild the index, reusing cached entries for unchanged files.
pub fn build_index_incremental(old_cache: &FileCache) -> (Vec<FnEntry>, Vec<TypeEntry>, Vec<TraitEntry>, Vec<ImplEntry>, FileCache) {
    let workspace = find_workspace_root();
    let roots: Vec<(String, PathBuf)> = roots_from_env().unwrap_or_else(|| {
        DEFAULT_ROOTS
            .iter()
            .map(|(name, rel)| (name.to_string(), workspace.join(rel)))
            .collect()
    });

    let mut all_fns = Vec::new();
    let mut all_types = Vec::new();
    let mut all_traits = Vec::new();
    let mut all_impls = Vec::new();
    let mut new_cache = FileCache::new();
    let mut reparsed = 0usize;
    let mut cached = 0usize;

    for (crate_name, src_dir) in &roots {
        if !src_dir.is_dir() {
            tracing::warn!("Skipping {}: {} not found", crate_name, src_dir.display());
            continue;
        }

        let files = collect_rs_files(src_dir);

        for file_path in files {
            let mtime = match std::fs::metadata(&file_path).and_then(|m| m.modified()) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("Failed to stat {}: {}", file_path.display(), e);
                    continue;
                }
            };

            // Check cache: reuse if mtime matches
            if let Some((old_mtime, old_items)) = old_cache.get(&file_path) {
                if *old_mtime == mtime {
                    all_fns.extend(old_items.functions.iter().cloned());
                    all_types.extend(old_items.types.iter().cloned());
                    all_traits.extend(old_items.traits.iter().cloned());
                    all_impls.extend(old_items.impls.iter().cloned());
                    new_cache.insert(file_path, (mtime, old_items.clone()));
                    cached += 1;
                    continue;
                }
            }

            // Cache miss — re-parse
            let source = match std::fs::read_to_string(&file_path) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("Failed to read {}: {}", file_path.display(), e);
                    continue;
                }
            };

            let rel_path = file_path
                .strip_prefix(src_dir)
                .unwrap_or(&file_path)
                .to_string_lossy()
                .to_string();

            let module_path = module_path_from_rel(&rel_path);
            let display_path = format!("{}/src/{}", crate_name, rel_path);

            match parser::extract_items(&source, &display_path, crate_name, &module_path) {
                Ok(items) => {
                    all_fns.extend(items.functions.iter().cloned());
                    all_types.extend(items.types.iter().cloned());
                    all_traits.extend(items.traits.iter().cloned());
                    all_impls.extend(items.impls.iter().cloned());
                    new_cache.insert(file_path, (mtime, items));
                    reparsed += 1;
                }
                Err(e) => {
                    tracing::warn!("Parse error in {}: {}", display_path, e);
                    reparsed += 1;
                }
            }
        }
    }

    let deleted = old_cache.len().saturating_sub(cached);
    tracing::info!(
        "{} fns + {} types + {} traits + {} impls, {} reparsed, {} cached, {} deleted",
        all_fns.len(),
        all_types.len(),
        all_traits.len(),
        all_impls.len(),
        reparsed,
        cached,
        deleted,
    );
    (all_fns, all_types, all_traits, all_impls, new_cache)
}
