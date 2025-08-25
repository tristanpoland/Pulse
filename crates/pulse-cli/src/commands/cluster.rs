use anyhow::{Context, Result};

use crate::client::PulseClient;
use crate::commands::formatters::{format_cluster_info, print_formatted_output, print_info};

pub async fn execute(client: &PulseClient, output_format: &str) -> Result<()> {
    print_info("Fetching cluster information...");

    let cluster_info = client.get_cluster_info().await
        .context("Failed to get cluster information")?;

    let formatted_output = format_cluster_info(&cluster_info, output_format)
        .context("Failed to format cluster information")?;
    
    print_formatted_output(&formatted_output)
        .context("Failed to print formatted output")?;

    Ok(())
}