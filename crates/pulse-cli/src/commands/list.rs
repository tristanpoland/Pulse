use anyhow::{Context, Result};

use crate::client::PulseClient;
use crate::commands::formatters::{format_jobs, print_formatted_output, print_info};

pub async fn execute(
    client: &PulseClient,
    status_filter: Option<String>,
    output_format: &str,
    recent: Option<u64>,
) -> Result<()> {
    print_info("Fetching jobs from cluster...");

    // Get jobs from cluster
    let mut jobs = client.list_jobs(status_filter.as_deref()).await
        .context("Failed to fetch jobs from cluster")?;

    // Apply recent filter
    if let Some(recent_count) = recent {
        jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        jobs.truncate(recent_count as usize);
    }

    // Format and display
    let formatted_output = format_jobs(&jobs, output_format)
        .context("Failed to format job list")?;
    
    print_formatted_output(&formatted_output)
        .context("Failed to print formatted output")?;

    if jobs.is_empty() {
        print_info("No jobs found matching the criteria.");
    } else {
        print_info(&format!("Found {} job(s)", jobs.len()));
    }

    Ok(())
}