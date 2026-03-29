mod editor;
mod index;
mod indexer;
mod parser;
mod server;
mod types;
mod watcher;

pub static STANDALONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

use anyhow::Result;
use rmcp::{ServiceExt, transport::stdio};
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<()> {

    //  Start with empty index so the MCP server can accept connections immediately.
    //  Tools will wait on `ready_rx` before accessing the index.
    let shared_index = Arc::new(RwLock::new(index::Index::empty()));
    let (ready_tx, ready_rx) = watch::channel(false);

    //  Build initial index in the background
    let idx_for_init = shared_index.clone();
    tokio::spawn(async move {
        tracing::info!("Building index...");
        let (entries, type_entries, trait_entries, impl_entries, file_cache) =
            tokio::task::spawn_blocking(indexer::build_index)
                .await
                .expect("indexer panicked");
        let idx = index::Index::new(entries, type_entries, trait_entries, impl_entries, file_cache);
        tracing::info!(
            "Index ready ({} fns + {} types + {} traits + {} impls)",
            idx.len(),
            idx.type_len(),
            idx.trait_len(),
            idx.impl_len(),
        );
        match idx_for_init.write() {
            Ok(mut shared) => *shared = idx,
            Err(e) => tracing::error!("Failed to set initial index: {}", e),
        }
        let _ = ready_tx.send(true);
    });

    //  Spawn file watcher for auto-reindex
    let roots = indexer::resolve_roots();
    tokio::spawn(watcher::watch_and_reindex(shared_index.clone(), roots));

    let service = server::VerusMcpServer::new(shared_index, ready_rx)
        .serve(stdio())
        .await
        .inspect_err(|e| tracing::error!("serving error: {:?}", e))?;

    service.waiting().await?;
    Ok(())
}
