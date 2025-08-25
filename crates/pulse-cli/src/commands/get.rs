use anyhow::{Context, Result};

use crate::client::PulseClient;
use crate::commands::formatters::{format_job, print_formatted_output};

pub async fn execute(
    client: &PulseClient,
    job_id: &str,
    output_format: &str,
    include_tasks: bool,
) -> Result<()> {
    // Get job details
    let job_info = client.get_job(job_id).await
        .with_context(|| format!("Failed to get job: {}", job_id))?;

    // Get tasks if requested
    let tasks = if include_tasks {
        Some(client.get_job_tasks(job_id).await
            .context("Failed to get job tasks")?)
    } else {
        None
    };

    // Format and display
    let formatted_output = format_job(&job_info, tasks.as_deref(), output_format)
        .context("Failed to format job details")?;
    
    print_formatted_output(&formatted_output)
        .context("Failed to print formatted output")?;

    Ok(())
}