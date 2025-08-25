use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    error::Result,
    task::{TaskDefinition, TaskExecution},
};

#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute_task(
        &self,
        task: &TaskDefinition,
        execution: &mut TaskExecution,
        context: &ExecutionContext,
    ) -> Result<TaskExecutionResult>;

    async fn cancel_task(&self, execution_id: &Uuid) -> Result<()>;
    async fn get_execution_status(&self, execution_id: &Uuid) -> Result<Option<TaskExecutionStatus>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub job_id: Uuid,
    pub worker_id: Uuid,
    pub environment: HashMap<String, String>,
    pub task_outputs: HashMap<String, HashMap<String, serde_json::Value>>, // task_id -> outputs
    pub secrets: HashMap<String, crate::SecretManager>, // namespace -> secrets
    pub working_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub output: Option<HashMap<String, serde_json::Value>>, // Named outputs for data passing
    pub error_message: Option<String>,
    pub duration_ms: u64,
    pub artifacts: Vec<crate::Artifact>,
}

/// Simplified artifact representation for execution results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionArtifact {
    pub name: String,
    pub path: String,
    pub size_bytes: u64,
    pub content_type: Option<String>,
}

impl ExecutionArtifact {
    /// Convert to full Artifact with a specific task execution ID
    pub fn to_artifact(&self, task_execution_id: Uuid) -> crate::Artifact {
        crate::Artifact::new(
            &self.name,
            crate::ArtifactType::File, // Default to File type
            &self.path,
            self.size_bytes,
            task_execution_id,
        )
        .with_metadata(
            "content_type", 
            self.content_type.as_deref().unwrap_or_default()
        )
    }
}

impl From<ExecutionArtifact> for crate::Artifact {
    fn from(exec_artifact: ExecutionArtifact) -> Self {
        exec_artifact.to_artifact(uuid::Uuid::new_v4()) // Use random ID if no specific ID provided
    }
}

#[cfg(test)]
mod execution_artifact_tests {
    use super::*;
    
    #[test]
    fn test_execution_artifact_conversion() {
        let exec_artifact = ExecutionArtifact {
            name: "test.txt".to_string(),
            path: "/path/to/test.txt".to_string(),
            size_bytes: 1024,
            content_type: Some("text/plain".to_string()),
        };

        let task_id = Uuid::new_v4();
        let artifact = exec_artifact.to_artifact(task_id);

        assert_eq!(artifact.name, "test.txt");
        assert_eq!(artifact.path, "/path/to/test.txt");
        assert_eq!(artifact.size_bytes, 1024);
        assert_eq!(artifact.task_execution_id, task_id);
        assert_eq!(artifact.artifact_type, crate::ArtifactType::File);
        assert_eq!(artifact.metadata.get("content_type"), Some(&"text/plain".to_string()));
    }

    #[test]
    fn test_execution_artifact_from_conversion() {
        let exec_artifact = ExecutionArtifact {
            name: "log.txt".to_string(),
            path: "/path/to/log.txt".to_string(),
            size_bytes: 512,
            content_type: None,
        };

        let artifact: crate::Artifact = exec_artifact.into();
        assert_eq!(artifact.name, "log.txt");
        assert_eq!(artifact.path, "/path/to/log.txt");
        assert_eq!(artifact.size_bytes, 512);
        assert_eq!(artifact.artifact_type, crate::ArtifactType::File);
        assert_eq!(artifact.metadata.get("content_type"), Some(&"".to_string()));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskExecutionStatus {
    Pending,
    Running {
        started_at: chrono::DateTime<chrono::Utc>,
        progress: Option<f64>,
    },
    Completed {
        result: TaskExecutionResult,
    },
    Failed {
        error: String,
        result: Option<TaskExecutionResult>,
    },
    Cancelled,
}

#[async_trait]
pub trait ActionExecutor: Send + Sync {
    fn can_execute(&self, action: &str) -> bool;
    
    async fn execute_action(
        &self,
        action: &str,
        parameters: &HashMap<String, serde_json::Value>,
        context: &ExecutionContext,
    ) -> Result<TaskExecutionResult>;
}

pub struct ExecutorRegistry {
    executors: HashMap<String, Box<dyn ActionExecutor>>,
}

impl ExecutorRegistry {
    pub fn new() -> Self {
        Self {
            executors: HashMap::new(),
        }
    }

    pub fn register_executor(
        &mut self,
        name: String,
        executor: Box<dyn ActionExecutor>,
    ) {
        self.executors.insert(name, executor);
    }

    pub fn get_executor(&self, action: &str) -> Option<&dyn ActionExecutor> {
        self.executors
            .values()
            .find(|executor| executor.can_execute(action))
            .map(|e| e.as_ref())
    }
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_memory_mb: Option<u64>,
    pub max_cpu_cores: Option<f64>,
    pub max_disk_mb: Option<u64>,
    pub max_execution_time_seconds: Option<u64>,
    pub max_network_bandwidth_mbps: Option<u64>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: Some(1024), // 1GB default
            max_cpu_cores: Some(1.0),
            max_disk_mb: Some(10240), // 10GB default
            max_execution_time_seconds: Some(3600), // 1 hour default
            max_network_bandwidth_mbps: Some(100),
        }
    }
}