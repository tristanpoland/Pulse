use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json;
use std::io::{self, Write};

use crate::client::{ClusterInfo, JobInfo, TaskInfo, LogEntry, NodeInfo};

pub fn format_jobs(jobs: &[JobInfo], format: &str) -> Result<String> {
    match format {
        "json" => Ok(serde_json::to_string_pretty(jobs)?),
        "yaml" => Ok(serde_yaml::to_string(jobs)?),
        "table" | _ => Ok(format_jobs_table(jobs)),
    }
}

pub fn format_job(job: &JobInfo, tasks: Option<&[TaskInfo]>, format: &str) -> Result<String> {
    match format {
        "json" => {
            if let Some(task_list) = tasks {
                let combined = serde_json::json!({
                    "job": job,
                    "tasks": task_list
                });
                Ok(serde_json::to_string_pretty(&combined)?)
            } else {
                Ok(serde_json::to_string_pretty(job)?)
            }
        }
        "yaml" => {
            if let Some(task_list) = tasks {
                let combined = serde_yaml::Value::Mapping({
                    let mut map = serde_yaml::Mapping::new();
                    map.insert(
                        serde_yaml::Value::String("job".to_string()),
                        serde_yaml::to_value(job)?,
                    );
                    map.insert(
                        serde_yaml::Value::String("tasks".to_string()),
                        serde_yaml::to_value(task_list)?,
                    );
                    map
                });
                Ok(serde_yaml::to_string(&combined)?)
            } else {
                Ok(serde_yaml::to_string(job)?)
            }
        }
        "table" | _ => Ok(format_job_table(job, tasks)),
    }
}

pub fn format_cluster_info(cluster: &ClusterInfo, format: &str) -> Result<String> {
    match format {
        "json" => Ok(serde_json::to_string_pretty(cluster)?),
        "yaml" => Ok(serde_yaml::to_string(cluster)?),
        "table" | _ => Ok(format_cluster_table(cluster)),
    }
}

pub fn format_logs(logs: &[LogEntry], _format: &str) -> Result<String> {
    // Logs are always formatted as text for readability
    let mut output = String::new();
    for log in logs {
        output.push_str(&format!(
            "{} [{}] {}: {}\n",
            log.timestamp.format("%Y-%m-%d %H:%M:%S"),
            log.level,
            log.source,
            log.message
        ));
    }
    Ok(output)
}

fn format_jobs_table(jobs: &[JobInfo]) -> String {
    if jobs.is_empty() {
        return "No jobs found.".to_string();
    }

    let mut output = String::new();
    
    // Header
    output.push_str(&format!(
        "{:<36} {:<20} {:<12} {:<8} {:<8} {:<8} {:<20}\n",
        "JOB ID", "NAME", "STATUS", "TASKS", "DONE", "FAILED", "CREATED"
    ));
    
    output.push_str(&"-".repeat(112));
    output.push('\n');

    // Rows
    for job in jobs {
        let created_str = format_duration_ago(job.created_at);
        
        output.push_str(&format!(
            "{:<36} {:<20} {:<12} {:<8} {:<8} {:<8} {:<20}\n",
            job.id,
            truncate_string(&job.name, 20),
            format_job_status(&job.status),
            job.task_count,
            job.completed_tasks,
            job.failed_tasks,
            created_str,
        ));
    }

    output
}

fn format_job_table(job: &JobInfo, tasks: Option<&[TaskInfo]>) -> String {
    let mut output = String::new();
    
    // Job details
    output.push_str("Job Details:\n");
    output.push_str(&format!("  ID:          {}\n", job.id));
    output.push_str(&format!("  Name:        {}\n", job.name));
    output.push_str(&format!("  Status:      {}\n", format_job_status(&job.status)));
    output.push_str(&format!("  Created:     {}\n", job.created_at.format("%Y-%m-%d %H:%M:%S UTC")));
    
    if let Some(started) = job.started_at {
        output.push_str(&format!("  Started:     {}\n", started.format("%Y-%m-%d %H:%M:%S UTC")));
    }
    
    if let Some(completed) = job.completed_at {
        output.push_str(&format!("  Completed:   {}\n", completed.format("%Y-%m-%d %H:%M:%S UTC")));
    }
    
    output.push_str(&format!("  Tasks:       {} total, {} completed, {} failed\n", 
                             job.task_count, job.completed_tasks, job.failed_tasks));
    
    // Task details if provided
    if let Some(task_list) = tasks {
        output.push_str("\nTasks:\n");
        
        if task_list.is_empty() {
            output.push_str("  No tasks found.\n");
        } else {
            output.push_str(&format!(
                "  {:<20} {:<25} {:<12} {:<20} {:<36}\n",
                "ID", "NAME", "STATUS", "DURATION", "WORKER"
            ));
            output.push_str(&format!("  {}\n", "-".repeat(115)));
            
            for task in task_list {
                let duration = calculate_task_duration(task);
                let worker = task.worker_id.map(|id| id.to_string()).unwrap_or_else(|| "-".to_string());
                
                output.push_str(&format!(
                    "  {:<20} {:<25} {:<12} {:<20} {:<36}\n",
                    truncate_string(&task.id, 20),
                    truncate_string(&task.name, 25),
                    task.status,
                    duration,
                    truncate_string(&worker, 36),
                ));
                
                if let Some(error) = &task.error_message {
                    output.push_str(&format!("    Error: {}\n", error));
                }
            }
        }
    }
    
    output
}

fn format_cluster_table(cluster: &ClusterInfo) -> String {
    let mut output = String::new();
    
    // Cluster summary
    output.push_str("Cluster Status:\n");
    output.push_str(&format!("  Total Nodes:   {}\n", cluster.total_nodes));
    output.push_str(&format!("  Healthy Nodes: {}\n", cluster.healthy_nodes));
    
    if let Some(leader) = cluster.leader {
        output.push_str(&format!("  Leader:        {}\n", leader));
    } else {
        output.push_str("  Leader:        None\n");
    }
    
    // Node details
    output.push_str("\nNodes:\n");
    
    if cluster.nodes.is_empty() {
        output.push_str("  No nodes found.\n");
    } else {
        output.push_str(&format!(
            "  {:<36} {:<15} {:<20} {:<10} {:<10} {:<20}\n",
            "NODE ID", "NAME", "ADDRESS", "STATUS", "TASKS", "LAST SEEN"
        ));
        output.push_str(&format!("  {}\n", "-".repeat(115)));
        
        for node in &cluster.nodes {
            let last_seen = format_duration_ago(node.last_seen);
            let task_info = format!("{}/{}", node.running_tasks, node.max_tasks);
            
            output.push_str(&format!(
                "  {:<36} {:<15} {:<20} {:<10} {:<10} {:<20}\n",
                node.id,
                truncate_string(&node.name, 15),
                truncate_string(&node.address, 20),
                node.status,
                task_info,
                last_seen,
            ));
        }
    }
    
    output
}

fn format_job_status(status: &pulse_core::JobStatus) -> String {
    match status {
        pulse_core::JobStatus::Pending => "pending".to_string(),
        pulse_core::JobStatus::Running => "running".to_string(),
        pulse_core::JobStatus::Completed => "completed".to_string(),
        pulse_core::JobStatus::Failed => "failed".to_string(),
        pulse_core::JobStatus::Cancelled => "cancelled".to_string(),
    }
}

fn calculate_task_duration(task: &TaskInfo) -> String {
    match (task.started_at, task.completed_at) {
        (Some(start), Some(end)) => {
            let duration = end - start;
            format_duration(duration)
        }
        (Some(start), None) => {
            let duration = Utc::now() - start;
            format!("{} (running)", format_duration(duration))
        }
        _ => "-".to_string(),
    }
}

fn format_duration(duration: chrono::Duration) -> String {
    let total_seconds = duration.num_seconds();
    
    if total_seconds < 60 {
        format!("{}s", total_seconds)
    } else if total_seconds < 3600 {
        let minutes = total_seconds / 60;
        let seconds = total_seconds % 60;
        format!("{}m{}s", minutes, seconds)
    } else {
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        format!("{}h{}m", hours, minutes)
    }
}

fn format_duration_ago(timestamp: DateTime<Utc>) -> String {
    let now = Utc::now();
    let duration = now - timestamp;
    
    let total_seconds = duration.num_seconds();
    
    if total_seconds < 60 {
        format!("{}s ago", total_seconds)
    } else if total_seconds < 3600 {
        let minutes = total_seconds / 60;
        format!("{}m ago", minutes)
    } else if total_seconds < 86400 {
        let hours = total_seconds / 3600;
        format!("{}h ago", hours)
    } else {
        let days = total_seconds / 86400;
        format!("{}d ago", days)
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

pub fn print_formatted_output(output: &str) -> Result<()> {
    print!("{}", output);
    io::stdout().flush()?;
    Ok(())
}

pub fn print_success(message: &str) {
    println!("✅ {}", message);
}

pub fn print_error(message: &str) {
    eprintln!("❌ {}", message);
}

pub fn print_warning(message: &str) {
    println!("⚠️  {}", message);
}

pub fn print_info(message: &str) {
    println!("ℹ️  {}", message);
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::JobStatus;
    use uuid::Uuid;

    fn create_test_job() -> JobInfo {
        JobInfo {
            id: Uuid::new_v4(),
            name: "test-job".to_string(),
            status: JobStatus::Running,
            created_at: Utc::now() - chrono::Duration::minutes(10),
            started_at: Some(Utc::now() - chrono::Duration::minutes(9)),
            completed_at: None,
            task_count: 3,
            completed_tasks: 1,
            failed_tasks: 0,
        }
    }

    fn create_test_tasks() -> Vec<TaskInfo> {
        vec![
            TaskInfo {
                id: "task-1".to_string(),
                name: "First Task".to_string(),
                status: "completed".to_string(),
                started_at: Some(Utc::now() - chrono::Duration::minutes(8)),
                completed_at: Some(Utc::now() - chrono::Duration::minutes(6)),
                worker_id: Some(Uuid::new_v4()),
                error_message: None,
            },
            TaskInfo {
                id: "task-2".to_string(),
                name: "Second Task".to_string(),
                status: "running".to_string(),
                started_at: Some(Utc::now() - chrono::Duration::minutes(5)),
                completed_at: None,
                worker_id: Some(Uuid::new_v4()),
                error_message: None,
            },
        ]
    }

    #[test]
    fn test_format_jobs_table() {
        let jobs = vec![create_test_job()];
        let output = format_jobs(&jobs, "table").unwrap();
        
        assert!(output.contains("JOB ID"));
        assert!(output.contains("NAME"));
        assert!(output.contains("STATUS"));
        assert!(output.contains("test-job"));
    }

    #[test]
    fn test_format_jobs_json() {
        let jobs = vec![create_test_job()];
        let output = format_jobs(&jobs, "json").unwrap();
        
        assert!(output.contains("test-job"));
        assert!(serde_json::from_str::<Vec<JobInfo>>(&output).is_ok());
    }

    #[test]
    fn test_format_job_with_tasks() {
        let job = create_test_job();
        let tasks = create_test_tasks();
        let output = format_job(&job, Some(&tasks), "table").unwrap();
        
        assert!(output.contains("Job Details:"));
        assert!(output.contains("Tasks:"));
        assert!(output.contains("task-1"));
        assert!(output.contains("task-2"));
    }

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("short", 10), "short");
        assert_eq!(truncate_string("very long string", 10), "very lo...");
        assert_eq!(truncate_string("exact", 5), "exact");
    }

    #[test]
    fn test_format_duration() {
        let duration = chrono::Duration::seconds(30);
        assert_eq!(format_duration(duration), "30s");
        
        let duration = chrono::Duration::seconds(90);
        assert_eq!(format_duration(duration), "1m30s");
        
        let duration = chrono::Duration::seconds(3661);
        assert_eq!(format_duration(duration), "1h1m");
    }

    #[test]
    fn test_calculate_task_duration() {
        let mut task = TaskInfo {
            id: "test".to_string(),
            name: "Test".to_string(),
            status: "completed".to_string(),
            started_at: Some(Utc::now() - chrono::Duration::seconds(30)),
            completed_at: Some(Utc::now()),
            worker_id: None,
            error_message: None,
        };
        
        let duration = calculate_task_duration(&task);
        assert!(duration.contains("30s"));
        
        // Test running task
        task.completed_at = None;
        let duration = calculate_task_duration(&task);
        assert!(duration.contains("running"));
        
        // Test not started task
        task.started_at = None;
        let duration = calculate_task_duration(&task);
        assert_eq!(duration, "-");
    }
}