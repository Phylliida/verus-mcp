mod index;
mod indexer;
mod parser;
mod server;
mod types;

use anyhow::Result;
use rmcp::{ServiceExt, transport::stdio};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Log to stderr so stdout stays clean for MCP JSON-RPC
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    tracing::info!("Building index...");
    let (entries, file_cache) = indexer::build_index();
    let idx = index::Index::new(entries, file_cache);
    tracing::info!("Index ready ({} items)", idx.len());

    let service = server::VerusMcpServer::new(idx)
        .serve(stdio())
        .await
        .inspect_err(|e| tracing::error!("serving error: {:?}", e))?;

    service.waiting().await?;
    Ok(())
}
