use anyhow::Result;
use clap::Parser;
use pulse_core::{NodeCapabilities, NodeInfo, NodeStatus, StorageConfig};
use pulse_executor::DistributedTaskExecutor;
use pulse_p2p::{DiscoveryService, P2PClusterManager, P2PNetwork, P2PService};
use pulse_storage::SledStorage;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

mod config;
mod scheduler;
mod worker;

use config::WorkerConfig;
use worker::PulseWorkerNode;

#[derive(Parser)]
#[command(name = "pulse-worker")]
#[command(about = "Pulse distributed workflow engine worker node")]
struct Args {
    #[arg(short, long, default_value = "worker.toml")]
    config: String,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "0.0.0.0:0")]
    listen_addr: String,

    #[arg(long)]
    bootstrap_peers: Vec<String>,

    #[arg(long, default_value = "./data")]
    data_dir: String,

    #[arg(long, default_value = "10")]
    max_tasks: usize,

    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(if args.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Pulse Worker Node");

    // Load configuration
    let mut config = if std::path::Path::new(&args.config).exists() {
        WorkerConfig::from_file(&args.config)?
    } else {
        warn!("Configuration file not found, using defaults");
        WorkerConfig::default()
    };

    // Override with command line arguments
    if let Some(node_id_str) = args.node_id {
        config.node.id = Uuid::parse_str(&node_id_str)?;
    }
    
    config.network.listen_address = args.listen_addr.clone();
    config.network.bootstrap_peers.extend(args.bootstrap_peers);
    config.storage.data_dir = args.data_dir;
    config.executor.max_concurrent_tasks = args.max_tasks;

    info!("Worker configuration loaded: {:#?}", config);

    // Create worker node
    let worker = PulseWorkerNode::new(config).await?;

    // Start the worker
    worker.start().await?;

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received");

    // Graceful shutdown
    worker.shutdown().await?;

    info!("Pulse Worker Node stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing() {
        let args = Args::parse_from(&[
            "pulse-worker",
            "--config", "test.toml",
            "--node-id", "550e8400-e29b-41d4-a716-446655440000",
            "--listen-addr", "127.0.0.1:8080",
            "--bootstrap-peers", "127.0.0.1:8081",
            "--bootstrap-peers", "127.0.0.1:8082",
            "--data-dir", "./test-data",
            "--max-tasks", "20",
            "--verbose",
        ]);

        assert_eq!(args.config, "test.toml");
        assert_eq!(args.node_id, Some("550e8400-e29b-41d4-a716-446655440000".to_string()));
        assert_eq!(args.listen_addr, "127.0.0.1:8080");
        assert_eq!(args.bootstrap_peers, vec!["127.0.0.1:8081", "127.0.0.1:8082"]);
        assert_eq!(args.data_dir, "./test-data");
        assert_eq!(args.max_tasks, 20);
        assert!(args.verbose);
    }
}