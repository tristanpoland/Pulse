use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{error::Result, Job, TaskExecution};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn store_job(&self, job: &Job) -> Result<()>;
    async fn get_job(&self, job_id: &Uuid) -> Result<Option<Job>>;
    async fn list_jobs(&self) -> Result<Vec<Job>>;
    async fn update_job(&self, job: &Job) -> Result<()>;
    async fn delete_job(&self, job_id: &Uuid) -> Result<()>;

    async fn store_task_execution(&self, execution: &TaskExecution) -> Result<()>;
    async fn get_task_execution(&self, execution_id: &Uuid) -> Result<Option<TaskExecution>>;
    async fn get_task_executions_for_job(&self, job_id: &Uuid) -> Result<Vec<TaskExecution>>;
    async fn update_task_execution(&self, execution: &TaskExecution) -> Result<()>;

    async fn store_key_value(&self, key: &str, value: &[u8]) -> Result<()>;
    async fn get_key_value(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn delete_key(&self, key: &str) -> Result<()>;
    async fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEvent {
    pub id: Uuid,
    pub node_id: Uuid,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub operation: ReplicationOperation,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationOperation {
    StoreJob,
    UpdateJob,
    DeleteJob,
    StoreTaskExecution,
    UpdateTaskExecution,
    StoreKeyValue { key: String },
    DeleteKey { key: String },
}

#[async_trait]
pub trait ReplicatedStorage: Storage {
    async fn replicate_event(&self, event: &ReplicationEvent) -> Result<()>;
    async fn get_pending_events(&self, since: chrono::DateTime<chrono::Utc>) -> Result<Vec<ReplicationEvent>>;
    async fn mark_event_applied(&self, event_id: &Uuid) -> Result<()>;
    async fn get_node_status(&self) -> Result<NodeStorageStatus>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStorageStatus {
    pub node_id: Uuid,
    pub last_sync: chrono::DateTime<chrono::Utc>,
    pub storage_size: u64,
    pub pending_events: usize,
}

pub trait StorageFactory {
    type Storage: ReplicatedStorage;
    
    fn create_storage(&self, config: &StorageConfig) -> Result<Self::Storage>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub node_id: Uuid,
    pub replication_factor: usize,
    pub sync_interval_seconds: u64,
    pub max_storage_size_mb: Option<u64>,
    pub custom_options: HashMap<String, serde_json::Value>,
}