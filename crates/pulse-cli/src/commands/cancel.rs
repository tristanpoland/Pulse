use anyhow::{Context, Result};

use crate::client::PulseClient;
use crate::commands::formatters::{print_success, print_info};

pub async fn execute(client: &PulseClient, job_id: &str) -> Result<()> {
    print_info(&format!("Cancelling job: {}", job_id));

    client.cancel_job(job_id).await
        .with_context(|| format!("Failed to cancel job: {}", job_id))?;

    print_success(&format!("Job {} has been cancelled", job_id));

    Ok(())
}