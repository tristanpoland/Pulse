use anyhow::{Context, Result};
use pulse_parser::WorkflowParser;
use std::path::PathBuf;
use tracing::info;

use crate::client::PulseClient;
use crate::commands::formatters::{format_job, print_formatted_output, print_success, print_info};

pub async fn execute(
    client: &PulseClient,
    file: PathBuf,
    name: Option<String>,
    wait: bool,
    output_format: &str,
) -> Result<()> {
    info!("Submitting workflow file: {}", file.display());

    // Parse workflow file
    let parser = WorkflowParser::new();
    let workflow = parser.parse_from_file(&file)
        .with_context(|| format!("Failed to parse workflow file: {}", file.display()))?;

    // Convert workflow to job
    let mut job = parser.workflow_to_job(&workflow)
        .context("Failed to convert workflow to job")?;

    // Override name if provided
    if let Some(job_name) = name {
        job.name = job_name;
    }

    print_info(&format!("Submitting job: {} ({})", job.name, job.id));

    // Submit job
    let job_info = client.submit_job(job).await
        .context("Failed to submit job to cluster")?;

    print_success(&format!("Job submitted successfully: {}", job_info.id));

    // Format and display job info
    let formatted_output = format_job(&job_info, None, output_format)
        .context("Failed to format job output")?;
    
    print_formatted_output(&formatted_output)
        .context("Failed to print formatted output")?;

    // Wait for completion if requested
    if wait {
        print_info("Waiting for job completion...");
        wait_for_completion(client, &job_info.id.to_string()).await?;
    }

    Ok(())
}

async fn wait_for_completion(client: &PulseClient, job_id: &str) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    
    loop {
        interval.tick().await;
        
        match client.get_job(job_id).await {
            Ok(job_info) => {
                print_info(&format!("Job status: {} ({}/{} tasks completed)", 
                                  format_job_status(&job_info.status), 
                                  job_info.completed_tasks, 
                                  job_info.task_count));
                
                if job_info_is_finished(&job_info.status) {
                    if matches!(job_info.status, pulse_core::JobStatus::Completed) {
                        print_success(&format!("Job completed successfully in {} tasks", job_info.task_count));
                    } else {
                        print_error(&format!("Job finished with status: {}", format_job_status(&job_info.status)));
                        return Err(anyhow::anyhow!("Job failed or was cancelled"));
                    }
                    break;
                }
            }
            Err(e) => {
                print_warning(&format!("Failed to get job status: {}", e));
            }
        }
    }
    
    Ok(())
}

fn format_job_status(status: &pulse_core::JobStatus) -> &str {
    match status {
        pulse_core::JobStatus::Pending => "pending",
        pulse_core::JobStatus::Running => "running", 
        pulse_core::JobStatus::Completed => "completed",
        pulse_core::JobStatus::Failed => "failed",
        pulse_core::JobStatus::Cancelled => "cancelled",
    }
}

fn job_info_is_finished(status: &pulse_core::JobStatus) -> bool {
    matches!(status, 
        pulse_core::JobStatus::Completed | 
        pulse_core::JobStatus::Failed | 
        pulse_core::JobStatus::Cancelled
    )
}

// Import these from formatters.rs 
use crate::commands::formatters::{print_error, print_warning};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_format_job_status() {
        assert_eq!(format_job_status(&pulse_core::JobStatus::Pending), "pending");
        assert_eq!(format_job_status(&pulse_core::JobStatus::Running), "running");
        assert_eq!(format_job_status(&pulse_core::JobStatus::Completed), "completed");
        assert_eq!(format_job_status(&pulse_core::JobStatus::Failed), "failed");
        assert_eq!(format_job_status(&pulse_core::JobStatus::Cancelled), "cancelled");
    }

    #[test]
    fn test_job_info_is_finished() {
        assert!(!job_info_is_finished(&pulse_core::JobStatus::Pending));
        assert!(!job_info_is_finished(&pulse_core::JobStatus::Running));
        assert!(job_info_is_finished(&pulse_core::JobStatus::Completed));
        assert!(job_info_is_finished(&pulse_core::JobStatus::Failed));
        assert!(job_info_is_finished(&pulse_core::JobStatus::Cancelled));
    }

    #[tokio::test]
    async fn test_workflow_parsing() {
        let workflow_content = r#"
name: Test Workflow
version: "1.0"
on:
  event: push
jobs:
  test:
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4
      - name: Test
        run: echo "testing"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(workflow_content.as_bytes()).unwrap();
        
        let parser = WorkflowParser::new();
        let workflow = parser.parse_from_file(temp_file.path()).unwrap();
        
        assert_eq!(workflow.name, "Test Workflow");
        assert_eq!(workflow.jobs.len(), 1);
        assert!(workflow.jobs.contains_key("test"));
    }
}