use anyhow::{Context, Result};
use pulse_core::{Job, JobStatus, TaskExecution};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::CliConfig;

#[derive(Debug, Clone)]
pub struct PulseClient {
    config: CliConfig,
    // In a real implementation, this would contain HTTP client, gRPC client, etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub nodes: Vec<NodeInfo>,
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub leader: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: Uuid,
    pub name: String,
    pub address: String,
    pub status: String,
    pub running_tasks: usize,
    pub max_tasks: usize,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub id: Uuid,
    pub name: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub task_count: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub name: String,
    pub status: String,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub worker_id: Option<Uuid>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub level: String,
    pub message: String,
    pub source: String,
}

impl PulseClient {
    pub async fn new(config: CliConfig) -> Result<Self> {
        info!("Initializing Pulse client with endpoint: {}", config.cluster.endpoint);
        
        // In a real implementation, we would:
        // 1. Create HTTP/gRPC clients
        // 2. Test connection to cluster
        // 3. Authenticate if required
        
        Ok(Self { config })
    }

    pub async fn submit_job(&self, job: Job) -> Result<JobInfo> {
        info!("Submitting job: {} ({})", job.name, job.id);
        
        // In a real implementation, this would send the job to the cluster
        // via HTTP API, gRPC, or direct P2P communication
        
        // Simulate job submission
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        Ok(JobInfo {
            id: job.id,
            name: job.name,
            status: JobStatus::Pending,
            created_at: job.created_at,
            started_at: None,
            completed_at: None,
            task_count: job.tasks.len(),
            completed_tasks: 0,
            failed_tasks: 0,
        })
    }

    pub async fn list_jobs(&self, status_filter: Option<&str>) -> Result<Vec<JobInfo>> {
        debug!("Listing jobs with filter: {:?}", status_filter);
        
        // In a real implementation, this would query the cluster
        // Simulate with some example jobs
        let mut jobs = vec![
            JobInfo {
                id: Uuid::new_v4(),
                name: "example-ci".to_string(),
                status: JobStatus::Running,
                created_at: chrono::Utc::now() - chrono::Duration::minutes(10),
                started_at: Some(chrono::Utc::now() - chrono::Duration::minutes(9)),
                completed_at: None,
                task_count: 3,
                completed_tasks: 2,
                failed_tasks: 0,
            },
            JobInfo {
                id: Uuid::new_v4(),
                name: "deploy-prod".to_string(),
                status: JobStatus::Completed,
                created_at: chrono::Utc::now() - chrono::Duration::hours(1),
                started_at: Some(chrono::Utc::now() - chrono::Duration::minutes(59)),
                completed_at: Some(chrono::Utc::now() - chrono::Duration::minutes(45)),
                task_count: 5,
                completed_tasks: 5,
                failed_tasks: 0,
            },
        ];

        // Apply status filter
        if let Some(status) = status_filter {
            let filter_status = match status.to_lowercase().as_str() {
                "pending" => JobStatus::Pending,
                "running" => JobStatus::Running,
                "completed" => JobStatus::Completed,
                "failed" => JobStatus::Failed,
                "cancelled" => JobStatus::Cancelled,
                _ => return Err(anyhow::anyhow!("Invalid status filter: {}", status)),
            };
            
            jobs.retain(|job| job.status == filter_status);
        }

        Ok(jobs)
    }

    pub async fn get_job(&self, job_id: &str) -> Result<JobInfo> {
        debug!("Getting job details: {}", job_id);
        
        // In a real implementation, this would query the cluster
        // Simulate job retrieval
        
        let job_uuid = if let Ok(uuid) = Uuid::parse_str(job_id) {
            uuid
        } else {
            // Try to find by name (simplified)
            return Err(anyhow::anyhow!("Job not found: {}", job_id));
        };

        // Simulate job details
        Ok(JobInfo {
            id: job_uuid,
            name: "example-job".to_string(),
            status: JobStatus::Running,
            created_at: chrono::Utc::now() - chrono::Duration::minutes(5),
            started_at: Some(chrono::Utc::now() - chrono::Duration::minutes(4)),
            completed_at: None,
            task_count: 3,
            completed_tasks: 1,
            failed_tasks: 0,
        })
    }

    pub async fn get_job_tasks(&self, job_id: &str) -> Result<Vec<TaskInfo>> {
        debug!("Getting tasks for job: {}", job_id);
        
        // In a real implementation, this would query the cluster
        // Simulate task list
        Ok(vec![
            TaskInfo {
                id: "checkout".to_string(),
                name: "Checkout code".to_string(),
                status: "completed".to_string(),
                started_at: Some(chrono::Utc::now() - chrono::Duration::minutes(4)),
                completed_at: Some(chrono::Utc::now() - chrono::Duration::minutes(3)),
                worker_id: Some(Uuid::new_v4()),
                error_message: None,
            },
            TaskInfo {
                id: "build".to_string(),
                name: "Build application".to_string(),
                status: "running".to_string(),
                started_at: Some(chrono::Utc::now() - chrono::Duration::minutes(2)),
                completed_at: None,
                worker_id: Some(Uuid::new_v4()),
                error_message: None,
            },
            TaskInfo {
                id: "test".to_string(),
                name: "Run tests".to_string(),
                status: "pending".to_string(),
                started_at: None,
                completed_at: None,
                worker_id: None,
                error_message: None,
            },
        ])
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        info!("Cancelling job: {}", job_id);
        
        // In a real implementation, this would send a cancel request to the cluster
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        Ok(())
    }

    pub async fn get_cluster_info(&self) -> Result<ClusterInfo> {
        debug!("Getting cluster information");
        
        // In a real implementation, this would query the cluster
        // Simulate cluster info
        let nodes = vec![
            NodeInfo {
                id: Uuid::new_v4(),
                name: "worker-1".to_string(),
                address: "192.168.1.10:8080".to_string(),
                status: "online".to_string(),
                running_tasks: 2,
                max_tasks: 10,
                last_seen: chrono::Utc::now(),
            },
            NodeInfo {
                id: Uuid::new_v4(),
                name: "worker-2".to_string(),
                address: "192.168.1.11:8080".to_string(),
                status: "online".to_string(),
                running_tasks: 0,
                max_tasks: 10,
                last_seen: chrono::Utc::now() - chrono::Duration::seconds(30),
            },
            NodeInfo {
                id: Uuid::new_v4(),
                name: "worker-3".to_string(),
                address: "192.168.1.12:8080".to_string(),
                status: "offline".to_string(),
                running_tasks: 0,
                max_tasks: 10,
                last_seen: chrono::Utc::now() - chrono::Duration::minutes(5),
            },
        ];

        let healthy_nodes = nodes.iter().filter(|n| n.status == "online").count();

        Ok(ClusterInfo {
            total_nodes: nodes.len(),
            healthy_nodes,
            leader: Some(nodes[0].id),
            nodes,
        })
    }

    pub async fn get_logs(&self, job_id: &str, task_id: Option<&str>, lines: u32) -> Result<Vec<LogEntry>> {
        debug!("Getting logs for job: {}, task: {:?}, lines: {}", job_id, task_id, lines);
        
        // In a real implementation, this would query the cluster for logs
        // Simulate log entries
        let mut logs = vec![
            LogEntry {
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(5),
                level: "INFO".to_string(),
                message: "Job started".to_string(),
                source: "scheduler".to_string(),
            },
            LogEntry {
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(4),
                level: "INFO".to_string(),
                message: "Task checkout started".to_string(),
                source: "worker-1".to_string(),
            },
            LogEntry {
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(3),
                level: "INFO".to_string(),
                message: "Cloning repository...".to_string(),
                source: "task:checkout".to_string(),
            },
            LogEntry {
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(3),
                level: "INFO".to_string(),
                message: "Repository cloned successfully".to_string(),
                source: "task:checkout".to_string(),
            },
            LogEntry {
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(2),
                level: "INFO".to_string(),
                message: "Task build started".to_string(),
                source: "worker-1".to_string(),
            },
            LogEntry {
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(1),
                level: "INFO".to_string(),
                message: "Building application...".to_string(),
                source: "task:build".to_string(),
            },
        ];

        // Filter by task if specified
        if let Some(task) = task_id {
            let task_source = format!("task:{}", task);
            logs.retain(|log| log.source == task_source);
        }

        // Limit to requested number of lines
        if logs.len() > lines as usize {
            logs = logs.into_iter().rev().take(lines as usize).rev().collect();
        }

        Ok(logs)
    }

    pub async fn follow_logs(
        &self,
        job_id: &str,
        task_id: Option<&str>,
        mut callback: impl FnMut(LogEntry) + Send,
    ) -> Result<()> {
        info!("Following logs for job: {}, task: {:?}", job_id, task_id);
        
        // In a real implementation, this would establish a streaming connection
        // Simulate log following
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        
        for i in 0..10 {
            interval.tick().await;
            
            let log_entry = LogEntry {
                timestamp: chrono::Utc::now(),
                level: "INFO".to_string(),
                message: format!("Log message #{}", i + 1),
                source: task_id.map(|t| format!("task:{}", t)).unwrap_or_else(|| "job".to_string()),
            };
            
            callback(log_entry);
        }

        Ok(())
    }

    pub fn get_endpoint(&self) -> &str {
        &self.config.cluster.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliConfig;

    async fn create_test_client() -> PulseClient {
        let config = CliConfig::default();
        PulseClient::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_client_creation() {
        let client = create_test_client().await;
        assert!(!client.get_endpoint().is_empty());
    }

    #[tokio::test]
    async fn test_list_jobs() {
        let client = create_test_client().await;
        let jobs = client.list_jobs(None).await.unwrap();
        assert!(!jobs.is_empty());
    }

    #[tokio::test]
    async fn test_list_jobs_with_filter() {
        let client = create_test_client().await;
        let running_jobs = client.list_jobs(Some("running")).await.unwrap();
        assert!(running_jobs.iter().all(|job| job.status == JobStatus::Running));
    }

    #[tokio::test]
    async fn test_get_cluster_info() {
        let client = create_test_client().await;
        let cluster_info = client.get_cluster_info().await.unwrap();
        assert!(!cluster_info.nodes.is_empty());
        assert!(cluster_info.total_nodes > 0);
    }

    #[tokio::test]
    async fn test_get_logs() {
        let client = create_test_client().await;
        let logs = client.get_logs("test-job", None, 10).await.unwrap();
        assert!(!logs.is_empty());
        assert!(logs.len() <= 10);
    }

    #[tokio::test]
    async fn test_get_logs_with_task_filter() {
        let client = create_test_client().await;
        let logs = client.get_logs("test-job", Some("build"), 10).await.unwrap();
        // In our test implementation, this should return empty since we filter by task
        // In a real implementation, it would return logs for that specific task
    }
}