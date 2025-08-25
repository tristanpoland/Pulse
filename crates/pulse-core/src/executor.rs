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
    pub task_outputs: HashMap<String, serde_json::Value>,
    pub secrets: HashMap<String, String>,
    pub working_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub output: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub duration_ms: u64,
    pub artifacts: Vec<ExecutionArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionArtifact {
    pub name: String,
    pub path: String,
    pub size_bytes: u64,
    pub content_type: Option<String>,
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