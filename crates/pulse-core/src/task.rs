use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    pub id: Uuid,
    pub job_id: Uuid,
    pub task_id: String,
    pub status: TaskStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub worker_id: Option<Uuid>,
    pub error_message: Option<String>,
    pub output: Option<serde_json::Value>,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDefinition {
    pub id: String,
    pub name: String,
    pub command: Vec<String>,
    pub dependencies: Vec<String>,
    pub environment: HashMap<String, String>,
    pub timeout_seconds: Option<u64>,
    pub retry_limit: u32, // Deprecated - use retry_config instead
    pub retry_config: Option<crate::RetryConfig>,
    pub needs: Vec<String>, // Tasks this task needs to complete first
    pub uses: Option<String>, // Action to use (like GitHub Actions)
    pub with: Option<HashMap<String, serde_json::Value>>, // Parameters for the action
    pub artifact_paths: Option<Vec<String>>, // Paths/patterns for artifact collection
    pub outputs: Option<HashMap<String, String>>, // Output definitions for data passing
}

impl TaskExecution {
    pub fn new(job_id: Uuid, task_def: &TaskDefinition) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_id,
            task_id: task_def.id.clone(),
            status: TaskStatus::Pending,
            started_at: None,
            completed_at: None,
            worker_id: None,
            error_message: None,
            output: None,
            retry_count: 0,
        }
    }

    pub fn start(&mut self, worker_id: Uuid) {
        self.status = TaskStatus::Running;
        self.started_at = Some(Utc::now());
        self.worker_id = Some(worker_id);
    }

    pub fn complete(&mut self, output: Option<serde_json::Value>) {
        self.status = TaskStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.output = output;
    }

    pub fn fail(&mut self, error: String) {
        self.status = TaskStatus::Failed;
        self.completed_at = Some(Utc::now());
        self.error_message = Some(error);
    }

    pub fn retry(&mut self) {
        self.status = TaskStatus::Pending;
        self.retry_count += 1;
        self.worker_id = None;
        self.started_at = None;
        self.completed_at = None;
        self.error_message = None;
    }

    pub fn can_retry(&self) -> bool {
        self.retry_count < self.retry_limit()
    }

    pub fn retry_limit(&self) -> u32 {
        // This would normally come from the task definition
        3
    }
}

impl TaskDefinition {
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            command: Vec::new(),
            dependencies: Vec::new(),
            environment: HashMap::new(),
            timeout_seconds: None,
            retry_limit: 3,
            retry_config: None,
            needs: Vec::new(),
            uses: None,
            with: None,
            artifact_paths: None,
            outputs: None,
        }
    }

    pub fn with_command(mut self, command: Vec<String>) -> Self {
        self.command = command;
        self
    }

    pub fn with_dependencies(mut self, deps: Vec<String>) -> Self {
        self.dependencies = deps;
        self
    }

    pub fn with_needs(mut self, needs: Vec<String>) -> Self {
        self.needs = needs;
        self
    }

    pub fn with_environment(mut self, env: HashMap<String, String>) -> Self {
        self.environment = env;
        self
    }

    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    pub fn with_retry_limit(mut self, limit: u32) -> Self {
        self.retry_limit = limit;
        self
    }

    pub fn with_artifacts(mut self, paths: Vec<String>) -> Self {
        self.artifact_paths = Some(paths);
        self
    }

    pub fn with_outputs(mut self, outputs: HashMap<String, String>) -> Self {
        self.outputs = Some(outputs);
        self
    }

    pub fn with_retry_config(mut self, config: crate::RetryConfig) -> Self {
        self.retry_config = Some(config);
        self
    }
}