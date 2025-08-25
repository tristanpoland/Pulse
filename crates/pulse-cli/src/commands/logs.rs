use anyhow::{Context, Result};

use crate::client::PulseClient;
use crate::commands::formatters::{format_logs, print_formatted_output, print_info};

pub async fn execute(
    client: &PulseClient,
    job_id: &str,
    task_id: Option<String>,
    follow: bool,
    lines: u32,
) -> Result<()> {
    if follow {
        print_info(&format!("Following logs for job: {}", job_id));
        follow_logs(client, job_id, task_id.as_deref()).await
    } else {
        print_info(&format!("Fetching logs for job: {}", job_id));
        get_logs(client, job_id, task_id.as_deref(), lines).await
    }
}

async fn get_logs(
    client: &PulseClient,
    job_id: &str,
    task_id: Option<&str>,
    lines: u32,
) -> Result<()> {
    let logs = client.get_logs(job_id, task_id, lines).await
        .with_context(|| format!("Failed to get logs for job: {}", job_id))?;

    let formatted_output = format_logs(&logs, "text")
        .context("Failed to format logs")?;
    
    print_formatted_output(&formatted_output)
        .context("Failed to print logs")?;

    Ok(())
}

async fn follow_logs(
    client: &PulseClient,
    job_id: &str,
    task_id: Option<&str>,
) -> Result<()> {
    client.follow_logs(job_id, task_id, |log_entry| {
        let formatted = format!(
            "{} [{}] {}: {}",
            log_entry.timestamp.format("%Y-%m-%d %H:%M:%S"),
            log_entry.level,
            log_entry.source,
            log_entry.message
        );
        println!("{}", formatted);
    }).await
        .with_context(|| format!("Failed to follow logs for job: {}", job_id))?;

    Ok(())
}