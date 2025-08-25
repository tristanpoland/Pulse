use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::client::{ClusterInfo, JobInfo, LogEntry, TaskInfo};
use crate::config::CliConfig;

#[derive(Debug, Clone)]
pub struct ApiClient {
    client: Client,
    base_url: String,
    timeout: Duration,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobSubmissionRequest {
    pub job: pulse_core::Job,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobSubmissionResponse {
    pub job_id: Uuid,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogsRequest {
    pub job_id: Uuid,
    pub task_id: Option<String>,
    pub lines: u32,
    pub since: Option<chrono::DateTime<chrono::Utc>>,
}

impl ApiClient {
    pub async fn new(config: &CliConfig) -> Result<Self> {
        let timeout = Duration::from_secs(config.cluster.timeout_seconds);
        
        let client = Client::builder()
            .timeout(timeout)
            .user_agent("pulse-cli/0.1.0")
            .build()
            .context("Failed to create HTTP client")?;

        // Test connection to cluster
        let health_url = format!("{}/health", config.cluster.endpoint);
        let response = client
            .get(&health_url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("Failed to connect to cluster endpoint")?;

        if !response.status().is_success() {
            anyhow::bail!("Cluster endpoint health check failed: {}", response.status());
        }

        info!("Connected to cluster at: {}", config.cluster.endpoint);

        Ok(Self {
            client,
            base_url: config.cluster.endpoint.clone(),
            timeout,
        })
    }

    pub async fn submit_job(&self, job: &pulse_core::Job) -> Result<JobInfo> {
        let url = format!("{}/api/v1/jobs", self.base_url);
        let request = JobSubmissionRequest { job: job.clone() };

        debug!("Submitting job to: {}", url);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to submit job")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("Job submission failed: {} - {}", response.status(), error_text);
        }

        let api_response: ApiResponse<JobInfo> = response
            .json()
            .await
            .context("Failed to parse job submission response")?;

        if !api_response.success {
            anyhow::bail!(
                "Job submission failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        api_response
            .data
            .context("Job submission response missing data")
    }

    pub async fn list_jobs(&self, status_filter: Option<&str>) -> Result<Vec<JobInfo>> {
        let mut url = format!("{}/api/v1/jobs", self.base_url);
        
        if let Some(status) = status_filter {
            url.push_str(&format!("?status={}", status));
        }

        debug!("Listing jobs from: {}", url);

        let response = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to list jobs")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to list jobs: {} - {}", response.status(), error_text);
        }

        let api_response: ApiResponse<Vec<JobInfo>> = response
            .json()
            .await
            .context("Failed to parse jobs list response")?;

        if !api_response.success {
            anyhow::bail!(
                "List jobs failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        Ok(api_response.data.unwrap_or_default())
    }

    pub async fn get_job(&self, job_id: &str) -> Result<JobInfo> {
        let url = format!("{}/api/v1/jobs/{}", self.base_url, job_id);

        debug!("Getting job from: {}", url);

        let response = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to get job")?;

        match response.status() {
            StatusCode::NOT_FOUND => anyhow::bail!("Job not found: {}", job_id),
            status if !status.is_success() => {
                let error_text = response.text().await.unwrap_or_default();
                anyhow::bail!("Failed to get job: {} - {}", status, error_text);
            }
            _ => {}
        }

        let api_response: ApiResponse<JobInfo> = response
            .json()
            .await
            .context("Failed to parse job response")?;

        if !api_response.success {
            anyhow::bail!(
                "Get job failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        api_response.data.context("Job response missing data")
    }

    pub async fn get_job_tasks(&self, job_id: &str) -> Result<Vec<TaskInfo>> {
        let url = format!("{}/api/v1/jobs/{}/tasks", self.base_url, job_id);

        debug!("Getting job tasks from: {}", url);

        let response = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to get job tasks")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to get job tasks: {} - {}", response.status(), error_text);
        }

        let api_response: ApiResponse<Vec<TaskInfo>> = response
            .json()
            .await
            .context("Failed to parse job tasks response")?;

        if !api_response.success {
            anyhow::bail!(
                "Get job tasks failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        Ok(api_response.data.unwrap_or_default())
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        let url = format!("{}/api/v1/jobs/{}/cancel", self.base_url, job_id);

        debug!("Cancelling job at: {}", url);

        let response = self
            .client
            .post(&url)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to cancel job")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to cancel job: {} - {}", response.status(), error_text);
        }

        let api_response: ApiResponse<()> = response
            .json()
            .await
            .context("Failed to parse job cancel response")?;

        if !api_response.success {
            anyhow::bail!(
                "Cancel job failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        Ok(())
    }

    pub async fn get_cluster_info(&self) -> Result<ClusterInfo> {
        let url = format!("{}/api/v1/cluster", self.base_url);

        debug!("Getting cluster info from: {}", url);

        let response = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to get cluster info")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to get cluster info: {} - {}", response.status(), error_text);
        }

        let api_response: ApiResponse<ClusterInfo> = response
            .json()
            .await
            .context("Failed to parse cluster info response")?;

        if !api_response.success {
            anyhow::bail!(
                "Get cluster info failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        api_response.data.context("Cluster info response missing data")
    }

    pub async fn get_logs(
        &self,
        job_id: &str,
        task_id: Option<&str>,
        lines: u32,
    ) -> Result<Vec<LogEntry>> {
        let mut url = format!("{}/api/v1/jobs/{}/logs?lines={}", self.base_url, job_id, lines);
        
        if let Some(task) = task_id {
            url.push_str(&format!("&task_id={}", task));
        }

        debug!("Getting logs from: {}", url);

        let response = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .context("Failed to get logs")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to get logs: {} - {}", response.status(), error_text);
        }

        let api_response: ApiResponse<Vec<LogEntry>> = response
            .json()
            .await
            .context("Failed to parse logs response")?;

        if !api_response.success {
            anyhow::bail!(
                "Get logs failed: {}",
                api_response.error.unwrap_or_default()
            );
        }

        Ok(api_response.data.unwrap_or_default())
    }

    pub async fn follow_logs<F>(
        &self,
        job_id: &str,
        task_id: Option<&str>,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(LogEntry) + Send,
    {
        let mut ws_url = format!("ws://{}/api/v1/jobs/{}/logs/stream", 
                               self.base_url.replace("http://", "").replace("https://", ""), 
                               job_id);
        
        if let Some(task) = task_id {
            ws_url.push_str(&format!("?task_id={}", task));
        }

        info!("Connecting to log stream: {}", ws_url);

        let (ws_stream, _) = connect_async(&ws_url)
            .await
            .context("Failed to connect to log stream")?;

        let (mut write, mut read) = ws_stream.split();

        // Send initial subscription message
        let subscription = serde_json::json!({
            "action": "subscribe",
            "job_id": job_id,
            "task_id": task_id
        });

        write
            .send(Message::Text(subscription.to_string()))
            .await
            .context("Failed to send subscription message")?;

        // Read log messages
        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<LogEntry>(&text) {
                        Ok(log_entry) => callback(log_entry),
                        Err(e) => {
                            debug!("Failed to parse log entry: {} - {}", e, text);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Log stream closed");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn get_endpoint(&self) -> &str {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliConfig;

    async fn create_test_client() -> ApiClient {
        let config = CliConfig::default();
        // In tests, we would use a mock server
        ApiClient {
            client: Client::new(),
            base_url: "http://localhost:8080".to_string(),
            timeout: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn test_api_client_creation() {
        let client = create_test_client().await;
        assert_eq!(client.get_endpoint(), "http://localhost:8080");
    }

    #[test]
    fn test_api_response_serialization() {
        let response = ApiResponse {
            success: true,
            data: Some("test".to_string()),
            error: None,
            timestamp: chrono::Utc::now(),
        };

        let json = serde_json::to_string(&response).unwrap();
        let parsed: ApiResponse<String> = serde_json::from_str(&json).unwrap();
        
        assert!(parsed.success);
        assert_eq!(parsed.data, Some("test".to_string()));
    }
}