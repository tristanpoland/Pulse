use async_trait::async_trait;
use pulse_core::{
    Job, ReplicatedStorage, ReplicationEvent, NodeStorageStatus, Storage, StorageConfig,
    TaskExecution,
};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::{Result, StorageError};

const JOBS_TREE: &str = "jobs";
const TASK_EXECUTIONS_TREE: &str = "task_executions";
const REPLICATION_EVENTS_TREE: &str = "replication_events";
const METADATA_TREE: &str = "metadata";
const KV_TREE: &str = "key_value";

pub struct SledStorage {
    db: Arc<Db>,
    config: StorageConfig,
    jobs_tree: Tree,
    executions_tree: Tree,
    events_tree: Tree,
    metadata_tree: Tree,
    kv_tree: Tree,
    replication_state: Arc<RwLock<ReplicationState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicationState {
    last_event_id: Option<Uuid>,
    applied_events: std::collections::HashSet<Uuid>,
    pending_events: Vec<ReplicationEvent>,
}

impl SledStorage {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let db_path = Path::new(&config.data_dir).join("pulse-storage");
        std::fs::create_dir_all(&db_path)?;

        let db = sled::open(&db_path)?;
        
        let jobs_tree = db.open_tree(JOBS_TREE)?;
        let executions_tree = db.open_tree(TASK_EXECUTIONS_TREE)?;
        let events_tree = db.open_tree(REPLICATION_EVENTS_TREE)?;
        let metadata_tree = db.open_tree(METADATA_TREE)?;
        let kv_tree = db.open_tree(KV_TREE)?;

        let replication_state = Arc::new(RwLock::new(ReplicationState {
            last_event_id: None,
            applied_events: std::collections::HashSet::new(),
            pending_events: Vec::new(),
        }));

        let storage = Self {
            db: Arc::new(db),
            config,
            jobs_tree,
            executions_tree,
            events_tree,
            metadata_tree,
            kv_tree,
            replication_state,
        };

        storage.initialize().await?;
        Ok(storage)
    }

    async fn initialize(&self) -> Result<()> {
        info!("Initializing Sled storage for node {}", self.config.node_id);
        
        // Initialize node metadata
        let node_info = NodeStorageStatus {
            node_id: self.config.node_id,
            last_sync: chrono::Utc::now(),
            storage_size: 0,
            pending_events: 0,
        };
        
        let serialized = bincode::serialize(&node_info)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        
        self.metadata_tree.insert("node_status", serialized)?;
        self.db.flush_async().await?;
        
        Ok(())
    }

    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        bincode::serialize(value)
            .map_err(|e| StorageError::SerializationError(e.to_string()))
    }

    fn deserialize<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        bincode::deserialize(data)
            .map_err(|e| StorageError::SerializationError(e.to_string()))
    }

    fn job_key(job_id: &Uuid) -> Vec<u8> {
        format!("job:{}", job_id).into_bytes()
    }

    fn execution_key(execution_id: &Uuid) -> Vec<u8> {
        format!("execution:{}", execution_id).into_bytes()
    }

    fn execution_job_prefix(job_id: &Uuid) -> Vec<u8> {
        format!("job_executions:{}", job_id).into_bytes()
    }

    fn execution_job_key(job_id: &Uuid, execution_id: &Uuid) -> Vec<u8> {
        format!("job_executions:{}:{}", job_id, execution_id).into_bytes()
    }

    fn event_key(event_id: &Uuid) -> Vec<u8> {
        format!("event:{}", event_id).into_bytes()
    }

    async fn create_replication_event(
        &self,
        operation: pulse_core::ReplicationOperation,
        data: Vec<u8>,
    ) -> Result<ReplicationEvent> {
        let event = ReplicationEvent {
            id: Uuid::new_v4(),
            node_id: self.config.node_id,
            timestamp: chrono::Utc::now(),
            operation,
            data,
        };

        let serialized = self.serialize(&event)?;
        self.events_tree.insert(Self::event_key(&event.id), serialized)?;
        
        // Update replication state
        let mut state = self.replication_state.write().await;
        state.pending_events.push(event.clone());
        state.last_event_id = Some(event.id);

        Ok(event)
    }
}

#[async_trait]
impl Storage for SledStorage {
    async fn store_job(&self, job: &Job) -> pulse_core::Result<()> {
        debug!("Storing job: {}", job.id);
        
        let key = Self::job_key(&job.id);
        let data = self.serialize(job)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.jobs_tree.insert(key, data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Create replication event
        let job_data = self.serialize(job)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.create_replication_event(
            pulse_core::ReplicationOperation::StoreJob,
            job_data,
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn get_job(&self, job_id: &Uuid) -> pulse_core::Result<Option<Job>> {
        let key = Self::job_key(job_id);
        
        match self.jobs_tree.get(key)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))? {
            Some(data) => {
                let job: Job = self.deserialize(&data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    async fn list_jobs(&self) -> pulse_core::Result<Vec<Job>> {
        let mut jobs = Vec::new();
        
        for result in self.jobs_tree.iter() {
            let (_, data) = result
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            let job: Job = self.deserialize(&data)
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            jobs.push(job);
        }
        
        Ok(jobs)
    }

    async fn update_job(&self, job: &Job) -> pulse_core::Result<()> {
        debug!("Updating job: {}", job.id);
        
        let key = Self::job_key(&job.id);
        let data = self.serialize(job)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.jobs_tree.insert(key, data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Create replication event
        let job_data = self.serialize(job)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.create_replication_event(
            pulse_core::ReplicationOperation::UpdateJob,
            job_data,
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn delete_job(&self, job_id: &Uuid) -> pulse_core::Result<()> {
        debug!("Deleting job: {}", job_id);
        
        let key = Self::job_key(job_id);
        self.jobs_tree.remove(key)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Also remove associated task executions
        let execution_prefix = Self::execution_job_prefix(job_id);
        let mut keys_to_remove = Vec::new();
        
        for result in self.executions_tree.scan_prefix(execution_prefix) {
            let (key, _) = result
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            keys_to_remove.push(key.to_vec());
        }
        
        for key in keys_to_remove {
            self.executions_tree.remove(key)
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        }

        // Create replication event
        let job_id_data = self.serialize(job_id)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.create_replication_event(
            pulse_core::ReplicationOperation::DeleteJob,
            job_id_data,
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn store_task_execution(&self, execution: &TaskExecution) -> pulse_core::Result<()> {
        debug!("Storing task execution: {}", execution.id);
        
        // Store by execution ID
        let exec_key = Self::execution_key(&execution.id);
        let exec_data = self.serialize(execution)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.executions_tree.insert(exec_key, exec_data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Store by job association
        let job_exec_key = Self::execution_job_key(&execution.job_id, &execution.id);
        let job_exec_data = self.serialize(execution)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.executions_tree.insert(job_exec_key, job_exec_data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Create replication event
        let execution_data = self.serialize(execution)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.create_replication_event(
            pulse_core::ReplicationOperation::StoreTaskExecution,
            execution_data,
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn get_task_execution(&self, execution_id: &Uuid) -> pulse_core::Result<Option<TaskExecution>> {
        let key = Self::execution_key(execution_id);
        
        match self.executions_tree.get(key)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))? {
            Some(data) => {
                let execution: TaskExecution = self.deserialize(&data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                Ok(Some(execution))
            }
            None => Ok(None),
        }
    }

    async fn get_task_executions_for_job(&self, job_id: &Uuid) -> pulse_core::Result<Vec<TaskExecution>> {
        let prefix = Self::execution_job_prefix(job_id);
        let mut executions = Vec::new();
        
        for result in self.executions_tree.scan_prefix(prefix) {
            let (_, data) = result
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            let execution: TaskExecution = self.deserialize(&data)
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            executions.push(execution);
        }
        
        Ok(executions)
    }

    async fn update_task_execution(&self, execution: &TaskExecution) -> pulse_core::Result<()> {
        debug!("Updating task execution: {}", execution.id);
        
        // Update by execution ID
        let exec_key = Self::execution_key(&execution.id);
        let exec_data = self.serialize(execution)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.executions_tree.insert(exec_key, exec_data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Update by job association
        let job_exec_key = Self::execution_job_key(&execution.job_id, &execution.id);
        let job_exec_data = self.serialize(execution)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.executions_tree.insert(job_exec_key, job_exec_data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Create replication event
        let execution_data = self.serialize(execution)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        self.create_replication_event(
            pulse_core::ReplicationOperation::UpdateTaskExecution,
            execution_data,
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn store_key_value(&self, key: &str, value: &[u8]) -> pulse_core::Result<()> {
        self.kv_tree.insert(key.as_bytes(), value)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Create replication event
        self.create_replication_event(
            pulse_core::ReplicationOperation::StoreKeyValue { key: key.to_string() },
            value.to_vec(),
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn get_key_value(&self, key: &str) -> pulse_core::Result<Option<Vec<u8>>> {
        match self.kv_tree.get(key.as_bytes())
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))? {
            Some(data) => Ok(Some(data.to_vec())),
            None => Ok(None),
        }
    }

    async fn delete_key(&self, key: &str) -> pulse_core::Result<()> {
        self.kv_tree.remove(key.as_bytes())
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        // Create replication event
        self.create_replication_event(
            pulse_core::ReplicationOperation::DeleteKey { key: key.to_string() },
            Vec::new(),
        ).await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        Ok(())
    }

    async fn list_keys_with_prefix(&self, prefix: &str) -> pulse_core::Result<Vec<String>> {
        let mut keys = Vec::new();
        
        for result in self.kv_tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = result
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            keys.push(key_str);
        }
        
        Ok(keys)
    }
}

#[async_trait]
impl ReplicatedStorage for SledStorage {
    async fn replicate_event(&self, event: &ReplicationEvent) -> pulse_core::Result<()> {
        debug!("Replicating event: {}", event.id);
        
        let mut state = self.replication_state.write().await;
        
        // Check if event was already applied
        if state.applied_events.contains(&event.id) {
            debug!("Event {} already applied, skipping", event.id);
            return Ok(());
        }

        // Apply the event based on its operation
        match &event.operation {
            pulse_core::ReplicationOperation::StoreJob => {
                let job: Job = self.deserialize(&event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                let key = Self::job_key(&job.id);
                self.jobs_tree.insert(key, &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
            pulse_core::ReplicationOperation::UpdateJob => {
                let job: Job = self.deserialize(&event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                let key = Self::job_key(&job.id);
                self.jobs_tree.insert(key, &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
            pulse_core::ReplicationOperation::DeleteJob => {
                let job_id: Uuid = self.deserialize(&event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                let key = Self::job_key(&job_id);
                self.jobs_tree.remove(key)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
            pulse_core::ReplicationOperation::StoreTaskExecution => {
                let execution: TaskExecution = self.deserialize(&event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                let exec_key = Self::execution_key(&execution.id);
                let job_exec_key = Self::execution_job_key(&execution.job_id, &execution.id);
                
                self.executions_tree.insert(exec_key, &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                self.executions_tree.insert(job_exec_key, &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
            pulse_core::ReplicationOperation::UpdateTaskExecution => {
                let execution: TaskExecution = self.deserialize(&event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                let exec_key = Self::execution_key(&execution.id);
                let job_exec_key = Self::execution_job_key(&execution.job_id, &execution.id);
                
                self.executions_tree.insert(exec_key, &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                self.executions_tree.insert(job_exec_key, &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
            pulse_core::ReplicationOperation::StoreKeyValue { key } => {
                self.kv_tree.insert(key.as_bytes(), &event.data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
            pulse_core::ReplicationOperation::DeleteKey { key } => {
                self.kv_tree.remove(key.as_bytes())
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            }
        }

        // Mark event as applied
        state.applied_events.insert(event.id);
        
        // Store the event
        let event_key = Self::event_key(&event.id);
        let event_data = self.serialize(event)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        self.events_tree.insert(event_key, event_data)
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;

        self.db.flush_async().await
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
        
        info!("Successfully replicated event: {}", event.id);
        Ok(())
    }

    async fn get_pending_events(&self, since: chrono::DateTime<chrono::Utc>) -> pulse_core::Result<Vec<ReplicationEvent>> {
        let mut events = Vec::new();
        
        for result in self.events_tree.iter() {
            let (_, data) = result
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            let event: ReplicationEvent = self.deserialize(&data)
                .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
            
            if event.timestamp > since {
                events.push(event);
            }
        }
        
        // Sort by timestamp
        events.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        
        Ok(events)
    }

    async fn mark_event_applied(&self, event_id: &Uuid) -> pulse_core::Result<()> {
        let mut state = self.replication_state.write().await;
        state.applied_events.insert(*event_id);
        Ok(())
    }

    async fn get_node_status(&self) -> pulse_core::Result<NodeStorageStatus> {
        match self.metadata_tree.get("node_status")
            .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))? {
            Some(data) => {
                let mut status: NodeStorageStatus = self.deserialize(&data)
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                
                // Update storage size
                status.storage_size = self.db.size_on_disk()
                    .map_err(|e| pulse_core::PulseError::StorageError(e.to_string()))?;
                
                // Update pending events count
                let state = self.replication_state.read().await;
                status.pending_events = state.pending_events.len();
                
                Ok(status)
            }
            None => {
                let status = NodeStorageStatus {
                    node_id: self.config.node_id,
                    last_sync: chrono::Utc::now(),
                    storage_size: 0,
                    pending_events: 0,
                };
                Ok(status)
            }
        }
    }
}