use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pulse_core::{Job, JobStatus};
use pulse_parser::WorkflowParser;
use std::path::PathBuf;
use tracing::info;
use uuid::Uuid;

mod api;
mod client;
mod commands;
mod config;

use client::PulseClient;
use config::CliConfig;

#[derive(Parser)]
#[command(name = "pulse")]
#[command(about = "Pulse distributed workflow engine CLI")]
#[command(version = "0.1.0")]
struct Args {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, default_value = "pulse-cli.toml")]
    config: String,

    #[arg(long)]
    cluster_endpoint: Option<String>,

    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Submit a workflow for execution
    Submit {
        /// Path to the workflow YAML file
        file: PathBuf,

        /// Optional name for the job
        #[arg(long)]
        name: Option<String>,

        /// Wait for job completion
        #[arg(long)]
        wait: bool,

        /// Output format (json, yaml, table)
        #[arg(long, default_value = "table")]
        output: String,
    },

    /// List jobs and their status
    List {
        /// Filter by job status
        #[arg(long)]
        status: Option<String>,

        /// Output format (json, yaml, table)
        #[arg(long, default_value = "table")]
        output: String,

        /// Show only recent jobs
        #[arg(long)]
        recent: Option<u64>,
    },

    /// Get details about a specific job
    Get {
        /// Job ID or name
        job_id: String,

        /// Output format (json, yaml, table)
        #[arg(long, default_value = "table")]
        output: String,

        /// Show task executions
        #[arg(long)]
        tasks: bool,
    },

    /// Cancel a running job
    Cancel {
        /// Job ID or name
        job_id: String,
    },

    /// Show cluster status and nodes
    Cluster {
        /// Output format (json, yaml, table)
        #[arg(long, default_value = "table")]
        output: String,
    },

    /// Show logs for a job or task
    Logs {
        /// Job ID
        job_id: String,

        /// Task ID (optional)
        #[arg(long)]
        task: Option<String>,

        /// Follow logs
        #[arg(short, long)]
        follow: bool,

        /// Number of lines to show
        #[arg(long, default_value = "100")]
        lines: u32,
    },

    /// Validate a workflow file
    Validate {
        /// Path to the workflow YAML file
        file: PathBuf,
    },

    /// Generate example workflow files
    Examples {
        /// Type of example (simple, complex, docker)
        #[arg(default_value = "simple")]
        example_type: String,

        /// Output file path
        #[arg(short, long, default_value = "example-workflow.yml")]
        output: PathBuf,
    },

    /// Show CLI configuration
    Config {
        /// Show configuration
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Show current configuration
    Show,
    /// Set configuration value
    Set { key: String, value: String },
    /// Initialize configuration file
    Init,
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

    // Load configuration
    let config = CliConfig::load(&args.config).await.unwrap_or_else(|_| {
        if args.verbose {
            eprintln!("Warning: Could not load config file, using defaults");
        }
        CliConfig::default()
    });

    // Override cluster endpoint if provided
    let mut config = config;
    if let Some(endpoint) = args.cluster_endpoint {
        config.cluster.endpoint = endpoint;
    }

    // Create client
    let client = PulseClient::new(config.clone()).await?;

    // Execute command
    match args.command {
        Commands::Submit { file, name, wait, output } => {
            commands::submit::execute(&client, file, name, wait, &output).await?;
        }
        Commands::List { status, output, recent } => {
            commands::list::execute(&client, status, &output, recent).await?;
        }
        Commands::Get { job_id, output, tasks } => {
            commands::get::execute(&client, &job_id, &output, tasks).await?;
        }
        Commands::Cancel { job_id } => {
            commands::cancel::execute(&client, &job_id).await?;
        }
        Commands::Cluster { output } => {
            commands::cluster::execute(&client, &output).await?;
        }
        Commands::Logs { job_id, task, follow, lines } => {
            commands::logs::execute(&client, &job_id, task, follow, lines).await?;
        }
        Commands::Validate { file } => {
            commands::validate::execute(file).await?;
        }
        Commands::Examples { example_type, output } => {
            commands::examples::execute(&example_type, output).await?;
        }
        Commands::Config { action } => {
            match action {
                ConfigAction::Show => {
                    commands::config::show(&config).await?;
                }
                ConfigAction::Set { key, value } => {
                    commands::config::set(&args.config, &key, &value).await?;
                }
                ConfigAction::Init => {
                    commands::config::init(&args.config).await?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        let args = Args::parse_from(&[
            "pulse",
            "submit",
            "workflow.yml",
            "--name", "test-job",
            "--wait",
            "--output", "json",
        ]);

        match args.command {
            Commands::Submit { file, name, wait, output } => {
                assert_eq!(file, PathBuf::from("workflow.yml"));
                assert_eq!(name, Some("test-job".to_string()));
                assert!(wait);
                assert_eq!(output, "json");
            }
            _ => panic!("Expected Submit command"),
        }
    }

    #[test]
    fn test_list_command_parsing() {
        let args = Args::parse_from(&[
            "pulse",
            "list",
            "--status", "running",
            "--output", "table",
            "--recent", "10",
        ]);

        match args.command {
            Commands::List { status, output, recent } => {
                assert_eq!(status, Some("running".to_string()));
                assert_eq!(output, "table");
                assert_eq!(recent, Some(10));
            }
            _ => panic!("Expected List command"),
        }
    }

    #[test]
    fn test_get_command_parsing() {
        let args = Args::parse_from(&[
            "pulse",
            "get",
            "job-123",
            "--tasks",
            "--output", "json",
        ]);

        match args.command {
            Commands::Get { job_id, output, tasks } => {
                assert_eq!(job_id, "job-123");
                assert_eq!(output, "json");
                assert!(tasks);
            }
            _ => panic!("Expected Get command"),
        }
    }
}