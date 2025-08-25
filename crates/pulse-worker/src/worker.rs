use anyhow::{Context, Result};
use pulse_core::{Job, NodeInfo, NodeStatus, TaskExecution, TaskStatus};
use pulse_executor::{DistributedTaskExecutor, ExecutorPool};
use pulse_p2p::{
    BootstrapService, DiscoveryService, Message, MessageEnvelope, P2PClusterManager, P2PNetwork,
    P2PService,
};
use pulse_storage::SledStorage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::scheduler::TaskSchedulingEngine;

pub struct PulseWorkerNode {
    config: WorkerConfig,
    node_info: NodeInfo,
    storage: Arc<SledStorage>,
    executor_pool: Arc<ExecutorPool>,
    scheduler: Arc<TaskSchedulingEngine>,
    p2p_network: Option<P2PNetwork>,
    cluster_manager: Option<Arc<P2PClusterManager<P2PService>>>,
    discovery_service: Option<Arc<DiscoveryService<P2PService>>>,
    task_queue: Arc<RwLock<Vec<TaskExecution>>>,
    running_tasks: Arc<RwLock<HashMap<Uuid, TaskExecution>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl PulseWorkerNode {
    pub async fn new(config: WorkerConfig) -> Result<Self> {
        info!("Initializing Pulse Worker Node: {}", config.node.name);

        // Validate configuration
        config.validate()
            .context("Invalid worker configuration")?;

        // Create storage
        let storage_config = config.create_storage_config();
        let storage = Arc::new(
            SledStorage::new(storage_config)
                .await
                .context("Failed to initialize storage")?,
        );

        // Create executor pool
        let executor_pool = Arc::new(
            ExecutorPool::new(
                config.executor.max_concurrent_tasks,
                config.executor.working_directory.clone(),
            )
            .await,
        );

        // Create scheduler
        let scheduler = Arc::new(TaskSchedulingEngine::new(config.node.id));

        // Create node info
        let capabilities = config.create_node_capabilities();
        let node_info = NodeInfo {
            id: config.node.id,
            address: config.network.listen_address.clone(),
            status: NodeStatus::Online,
            capabilities,
            last_seen: chrono::Utc::now(),
            metadata: config.node.metadata.clone(),
        };

        Ok(Self {
            config,
            node_info,
            storage,
            executor_pool,
            scheduler,
            p2p_network: None,
            cluster_manager: None,
            discovery_service: None,
            task_queue: Arc::new(RwLock::new(Vec::new())),
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx: None,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting Pulse Worker Node: {}", self.node_info.id);

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Initialize P2P network
        let listen_addr = self.config.network.listen_address.parse()
            .context("Invalid listen address")?;

        let mut p2p_network = P2PNetwork::new(self.node_info.id, listen_addr)
            .await
            .context("Failed to initialize P2P network")?;

        // Set up message handlers
        self.setup_message_handlers(&mut p2p_network).await;

        // Create P2P service
        let p2p_service = Arc::new(P2PService::new(
            p2p_network.get_message_sender(),
            self.node_info.id,
        ));

        // Create discovery service
        let discovery_service = Arc::new(DiscoveryService::new(
            P2PService::new(p2p_network.get_message_sender(), self.node_info.id),
            self.node_info.id,
            self.node_info.clone(),
            self.config.discovery.discovery_interval_seconds,
        ));

        // Create cluster manager
        let cluster_manager = Arc::new(P2PClusterManager::new(
            p2p_service.clone(),
            discovery_service.clone(),
            self.node_info.id,
            self.node_info.clone(),
        ));

        // Store components
        self.discovery_service = Some(discovery_service.clone());
        self.cluster_manager = Some(cluster_manager.clone());

        // Start cluster manager
        cluster_manager
            .start()
            .await
            .context("Failed to start cluster manager")?;

        // Bootstrap from configured peers
        if !self.config.network.bootstrap_peers.is_empty() {
            let bootstrap_service = BootstrapService::new(
                self.config.network.bootstrap_peers.clone(),
                self.node_info.id,
            );
            
            match bootstrap_service.bootstrap(&*p2p_service).await {
                Ok(discovered_nodes) => {
                    info!("Bootstrapped and discovered {} nodes", discovered_nodes.len());
                }
                Err(e) => {
                    warn!("Bootstrap failed: {}", e);
                }
            }
        }

        // Start task processing
        self.start_task_processing().await;

        // Start network event loop
        let network_handle = {
            let mut network = p2p_network;
            tokio::spawn(async move {
                if let Err(e) = network.run().await {
                    error!("P2P network error: {}", e);
                }
            })
        };

        info!("Pulse Worker Node started successfully");

        // Wait for shutdown signal
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received");
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl+C received");
            }
        }

        // Cleanup
        network_handle.abort();

        Ok(())
    }

    async fn setup_message_handlers(&self, network: &mut P2PNetwork) {
        let task_queue = self.task_queue.clone();
        let storage = self.storage.clone();
        let node_id = self.node_info.id;

        // Handle job assignment messages
        network.register_message_handler(
            "assign_task".to_string(),
            move |envelope| {
                let task_queue = task_queue.clone();
                let storage = storage.clone();
                
                async move {
                    if let Message::AssignTask { execution } = envelope.payload {
                        info!("Received task assignment: {}", execution.task_id);
                        
                        // Add to task queue
                        let mut queue = task_queue.write().await;
                        queue.push(execution.clone());
                        
                        // Store in storage
                        if let Err(e) = storage.store_task_execution(&execution).await {
                            error!("Failed to store task execution: {}", e);
                        }
                        
                        debug!("Task added to queue: {}", execution.task_id);
                    }
                    
                    Ok(None)
                }
            },
        );

        // Handle job submission messages  
        network.register_message_handler(
            "submit_job".to_string(),
            move |envelope| {
                let storage = storage.clone();
                
                async move {
                    if let Message::SubmitJob { job } = envelope.payload {
                        info!("Received job submission: {}", job.name);
                        
                        // Store job
                        if let Err(e) = storage.store_job(&job).await {
                            error!("Failed to store job: {}", e);
                        } else {
                            info!("Job stored: {}", job.id);
                        }
                    }
                    
                    Ok(None)
                }
            },
        );

        // Handle discovery messages
        if let Some(discovery) = &self.discovery_service {
            let discovery_clone = discovery.clone();
            network.register_message_handler(
                "peer_discovery".to_string(),
                move |envelope| {
                    let discovery = discovery_clone.clone();
                    
                    async move {
                        discovery.handle_peer_discovery(envelope).await
                            .unwrap_or_else(|e| {
                                error!("Error handling peer discovery: {}", e);
                                None
                            })
                    }
                },
            );
        }

        // Handle cluster messages
        if let Some(cluster_manager) = &self.cluster_manager {
            let cluster_manager_clone = cluster_manager.clone();
            
            network.register_message_handler(
                "join_cluster".to_string(),
                move |envelope| {
                    let cluster_manager = cluster_manager_clone.clone();
                    
                    async move {
                        cluster_manager.handle_join_cluster(envelope).await
                            .unwrap_or_else(|e| {
                                error!("Error handling join cluster: {}", e);
                                None
                            })
                    }
                },
            );

            let cluster_manager_clone = cluster_manager.clone();
            network.register_message_handler(
                "leave_cluster".to_string(),
                move |envelope| {
                    let cluster_manager = cluster_manager_clone.clone();
                    
                    async move {
                        cluster_manager.handle_leave_cluster(envelope).await
                            .unwrap_or_else(|e| {
                                error!("Error handling leave cluster: {}", e);
                                None
                            })
                    }
                },
            );

            let cluster_manager_clone = cluster_manager.clone();
            network.register_message_handler(
                "cluster_update".to_string(),
                move |envelope| {
                    let cluster_manager = cluster_manager_clone.clone();
                    
                    async move {
                        cluster_manager.handle_cluster_update(envelope).await
                            .unwrap_or_else(|e| {
                                error!("Error handling cluster update: {}", e);
                                None
                            })
                    }
                },
            );
        }
    }

    async fn start_task_processing(&self) {
        let task_queue = self.task_queue.clone();
        let running_tasks = self.running_tasks.clone();
        let executor_pool = self.executor_pool.clone();
        let storage = self.storage.clone();
        let node_id = self.node_info.id;
        let max_concurrent = self.config.executor.max_concurrent_tasks;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(1000)); // Check every second

            loop {
                interval.tick().await;

                // Check if we can process more tasks
                let running_count = {
                    let running = running_tasks.read().await;
                    running.len()
                };

                if running_count >= max_concurrent {
                    continue; // At capacity
                }

                // Get next task from queue
                let task_execution = {
                    let mut queue = task_queue.write().await;
                    if queue.is_empty() {
                        continue;
                    }
                    queue.remove(0)
                };

                if task_execution.status != TaskStatus::Pending {
                    continue; // Skip non-pending tasks
                }

                info!("Processing task: {}", task_execution.task_id);

                // Get executor
                let executor = executor_pool.get_least_busy_executor().await;
                
                // Move task to running tasks
                {
                    let mut running = running_tasks.write().await;
                    running.insert(task_execution.id, task_execution.clone());
                }

                // Execute task
                let running_tasks_clone = running_tasks.clone();
                let storage_clone = storage.clone();
                let execution_id = task_execution.id;
                
                tokio::spawn(async move {
                    let result = Self::execute_task(
                        executor,
                        task_execution,
                        node_id,
                    ).await;

                    // Update running tasks
                    {
                        let mut running = running_tasks_clone.write().await;
                        running.remove(&execution_id);
                    }

                    // Store result
                    if let Ok(updated_execution) = result {
                        if let Err(e) = storage_clone.update_task_execution(&updated_execution).await {
                            error!("Failed to update task execution: {}", e);
                        }
                    }
                });
            }
        });
    }

    async fn execute_task(
        executor: Arc<DistributedTaskExecutor>,
        mut task_execution: TaskExecution,
        node_id: Uuid,
    ) -> Result<TaskExecution> {
        // Create execution context
        let context = pulse_core::ExecutionContext {
            job_id: task_execution.job_id,
            worker_id: node_id,
            environment: HashMap::new(), // TODO: Load from job/task
            task_outputs: HashMap::new(), // TODO: Load previous task outputs
            secrets: HashMap::new(),     // TODO: Load secrets
            working_directory: "./workspace".to_string(), // TODO: Make configurable
        };

        // Create task definition (simplified - in real implementation, load from storage)
        let task_definition = pulse_core::TaskDefinition::new(
            &task_execution.task_id,
            &task_execution.task_id,
        );

        // Execute task
        match executor.execute_task(&task_definition, &mut task_execution, &context).await {
            Ok(result) => {
                if result.success {
                    info!("Task {} completed successfully", task_execution.task_id);
                } else {
                    warn!("Task {} failed: {:?}", task_execution.task_id, result.error_message);
                }
            }
            Err(e) => {
                error!("Task {} execution error: {}", task_execution.task_id, e);
                task_execution.fail(e.to_string());
            }
        }

        Ok(task_execution)
    }

    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Pulse Worker Node: {}", self.node_info.id);

        // Leave cluster gracefully
        if let Some(cluster_manager) = &self.cluster_manager {
            if let Err(e) = cluster_manager.leave_cluster_gracefully().await {
                error!("Failed to leave cluster gracefully: {}", e);
            }
        }

        // Cancel running tasks
        let running_tasks = {
            let running = self.running_tasks.read().await;
            running.keys().cloned().collect::<Vec<_>>()
        };

        for task_id in running_tasks {
            info!("Cancelling running task: {}", task_id);
            // In a real implementation, we'd track and cancel running tasks
        }

        // Send shutdown signal
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(()).await;
        }

        info!("Pulse Worker Node shutdown complete");
        Ok(())
    }

    pub async fn get_status(&self) -> WorkerStatus {
        let running_tasks = self.running_tasks.read().await;
        let task_queue = self.task_queue.read().await;

        WorkerStatus {
            node_id: self.node_info.id,
            node_name: self.config.node.name.clone(),
            status: self.node_info.status.clone(),
            running_tasks: running_tasks.len(),
            queued_tasks: task_queue.len(),
            max_concurrent_tasks: self.config.executor.max_concurrent_tasks,
            uptime: chrono::Utc::now(), // Simplified - would track actual uptime
            last_heartbeat: chrono::Utc::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerStatus {
    pub node_id: Uuid,
    pub node_name: String,
    pub status: NodeStatus,
    pub running_tasks: usize,
    pub queued_tasks: usize,
    pub max_concurrent_tasks: usize,
    pub uptime: chrono::DateTime<chrono::Utc>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_worker() -> PulseWorkerNode {
        let temp_dir = tempdir().unwrap();
        
        let mut config = WorkerConfig::default();
        config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
        config.network.listen_address = "127.0.0.1:0".to_string();

        PulseWorkerNode::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_worker_creation() {
        let worker = create_test_worker().await;
        assert_eq!(worker.node_info.status, NodeStatus::Online);
        assert!(worker.node_info.capabilities.max_concurrent_tasks > 0);
    }

    #[tokio::test]
    async fn test_worker_status() {
        let worker = create_test_worker().await;
        let status = worker.get_status().await;
        
        assert_eq!(status.node_id, worker.node_info.id);
        assert_eq!(status.running_tasks, 0);
        assert_eq!(status.queued_tasks, 0);
        assert_eq!(status.status, NodeStatus::Online);
    }

    #[tokio::test]
    async fn test_task_queue_management() {
        let worker = create_test_worker().await;
        
        // Create test task execution
        let task_def = pulse_core::TaskDefinition::new("test-task", "Test Task");
        let task_execution = TaskExecution::new(Uuid::new_v4(), &task_def);
        
        // Add to queue
        {
            let mut queue = worker.task_queue.write().await;
            queue.push(task_execution.clone());
        }
        
        let status = worker.get_status().await;
        assert_eq!(status.queued_tasks, 1);
    }
}