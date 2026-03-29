use crate::index::Index;
use crate::indexer;
use notify::{EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

///  Watch source directories for `.rs` file changes and auto-reindex.
///  Debounces rapid changes with a 500ms delay.
pub async fn watch_and_reindex(index: Arc<RwLock<Index>>, roots: Vec<PathBuf>) {
    let (tx, mut rx) = mpsc::channel::<()>(64);

    let mut watcher = match notify::recommended_watcher(move |res: Result<notify::Event, _>| {
        if let Ok(event) = res {
            if matches!(
                event.kind,
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
            ) {
                if event
                    .paths
                    .iter()
                    .any(|p| p.extension().map_or(false, |e| e == "rs"))
                {
                    let _ = tx.blocking_send(());
                }
            }
        }
    }) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("Failed to create file watcher: {}", e);
            return;
        }
    };

    for root in &roots {
        if let Err(e) = watcher.watch(root, RecursiveMode::Recursive) {
            tracing::warn!("Failed to watch {}: {}", root.display(), e);
        } else {
            tracing::info!("Watching {}", root.display());
        }
    }

    loop {
        //  Wait for the first change event
        if rx.recv().await.is_none() {
            break; //  channel closed
        }
        //  Debounce: wait 500ms, drain any queued events
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        while rx.try_recv().is_ok() {}

        //  Incremental reindex
        let old_cache = {
            let idx = match index.read() {
                Ok(idx) => idx,
                Err(_) => continue,
            };
            idx.file_cache().clone()
        };

        let (entries, type_entries, trait_entries, impl_entries, new_cache) =
            indexer::build_index_incremental(&old_cache);
        let fn_count = entries.len();
        let type_count = type_entries.len();
        let new_index = Index::new(entries, type_entries, trait_entries, impl_entries, new_cache);

        match index.write() {
            Ok(mut idx) => {
                *idx = new_index;
                tracing::info!("Auto-reindexed: {} fns + {} types", fn_count, type_count);
            }
            Err(e) => {
                tracing::error!("Failed to acquire write lock for reindex: {}", e);
            }
        }
    }

    //  Keep _watcher alive — dropping it stops watching.
    //  This line is unreachable but prevents the watcher from being optimized away.
    drop(watcher);
}
