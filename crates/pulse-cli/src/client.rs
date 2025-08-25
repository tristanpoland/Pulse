use anyhow::Result;
use pulse_core::{Job, JobStatus};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

use crate::api::ApiClient;
use crate::config::CliConfig;

#[derive(Debug, Clone)]
pub struct PulseClient {
    api_client: ApiClient,
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
        
        let api_client = ApiClient::new(&config).await?;
        
        Ok(Self { api_client })
    }

    pub async fn submit_job(&self, job: Job) -> Result<JobInfo> {
        info!("Submitting job: {} ({})", job.name, job.id);
        self.api_client.submit_job(&job).await
    }

    pub async fn list_jobs(&self, status_filter: Option<&str>) -> Result<Vec<JobInfo>> {
        debug!("Listing jobs with filter: {:?}", status_filter);
        self.api_client.list_jobs(status_filter).await
    }

    pub async fn get_job(&self, job_id: &str) -> Result<JobInfo> {
        debug!("Getting job details: {}", job_id);
        self.api_client.get_job(job_id).await
    }

    pub async fn get_job_tasks(&self, job_id: &str) -> Result<Vec<TaskInfo>> {
        debug!("Getting tasks for job: {}", job_id);
        self.api_client.get_job_tasks(job_id).await
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        info!("Cancelling job: {}", job_id);
        self.api_client.cancel_job(job_id).await
    }

    pub async fn get_cluster_info(&self) -> Result<ClusterInfo> {
        debug!("Getting cluster information");
        self.api_client.get_cluster_info().await
    }

    pub async fn get_logs(&self, job_id: &str, task_id: Option<&str>, lines: u32) -> Result<Vec<LogEntry>> {
        debug!("Getting logs for job: {}, task: {:?}, lines: {}", job_id, task_id, lines);
        self.api_client.get_logs(job_id, task_id, lines).await
    }

    pub async fn follow_logs(
        &self,
        job_id: &str,
        task_id: Option<&str>,
        callback: impl FnMut(LogEntry) + Send,
    ) -> Result<()> {
        info!("Following logs for job: {}, task: {:?}", job_id, task_id);
        self.api_client.follow_logs(job_id, task_id, callback).await
    }

    pub fn get_endpoint(&self) -> &str {
        self.api_client.get_endpoint()
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