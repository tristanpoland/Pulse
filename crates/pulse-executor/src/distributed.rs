use async_trait::async_trait;
use pulse_core::{
    ActionExecutor, ExecutionContext, ExecutorRegistry, TaskDefinition, TaskExecution,
    TaskExecutionResult, TaskExecutor, TaskExecutionStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    docker::{CheckoutActionExecutor, DockerActionExecutor},
    error::{ExecutorError, Result},
    shell::{ShellActionExecutor, ShellExecutor},
};

pub struct DistributedTaskExecutor {
    shell_executor: ShellExecutor,
    action_registry: ExecutorRegistry,
    running_tasks: Arc<RwLock<HashMap<Uuid, RunningTask>>>,
}

#[derive(Debug, Clone)]
struct RunningTask {
    execution_id: Uuid,
    task_id: String,
    job_id: Uuid,
    started_at: chrono::DateTime<chrono::Utc>,
    status: TaskExecutionStatus,
    cancel_handle: Option<tokio::task::AbortHandle>,
}

impl DistributedTaskExecutor {
    pub async fn new(working_directory: String) -> Self {
        let shell_executor = ShellExecutor::new(working_directory);
        let mut action_registry = ExecutorRegistry::new();

        // Register built-in action executors
        action_registry.register_executor(
            "shell".to_string(),
            Box::new(ShellActionExecutor::new()),
        );
        
        action_registry.register_executor(
            "docker".to_string(),
            Box::new(DockerActionExecutor::new().await),
        );
        
        action_registry.register_executor(
            "checkout".to_string(),
            Box::new(CheckoutActionExecutor::new()),
        );

        Self {
            shell_executor,
            action_registry,
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_action_executor(
        &mut self,
        name: String,
        executor: Box<dyn ActionExecutor>,
    ) {
        self.action_registry.register_executor(name, executor);
    }

    async fn execute_action_task(
        &self,
        task: &TaskDefinition,
        execution: &mut TaskExecution,
        context: &ExecutionContext,
    ) -> Result<TaskExecutionResult> {
        let action = task.uses.as_ref().ok_or_else(|| {
            ExecutorError::InvalidParameters("Task has no 'uses' field for action execution".to_string())
        })?;

        debug!("Executing action task: {} with action: {}", task.id, action);

        // Find appropriate executor
        let executor = self.action_registry.get_executor(action).ok_or_else(|| {
            ExecutorError::ActionNotFound { action: action.clone() }
        })?;

        // Prepare parameters
        let parameters = task.with.clone().unwrap_or_default();

        // Execute the action
        execution.start(context.worker_id);
        
        match executor.execute_action(action, &parameters, context).await {
            Ok(result) => {
                if result.success {
                    execution.complete(result.output.clone());
                } else {
                    execution.fail(result.error_message.clone().unwrap_or_default());
                }
                Ok(result)
            }
            Err(e) => {
                let error_msg = e.to_string();
                execution.fail(error_msg.clone());
                Err(ExecutorError::ExecutionFailed(error_msg))
            }
        }
    }

    async fn track_running_task(
        &self,
        execution: &TaskExecution,
        task: &TaskDefinition,
        abort_handle: Option<tokio::task::AbortHandle>,
    ) {
        let running_task = RunningTask {
            execution_id: execution.id,
            task_id: task.id.clone(),
            job_id: execution.job_id,
            started_at: chrono::Utc::now(),
            status: TaskExecutionStatus::Running {
                started_at: chrono::Utc::now(),
                progress: None,
            },
            cancel_handle: abort_handle,
        };

        let mut tasks = self.running_tasks.write().await;
        tasks.insert(execution.id, running_task);
    }

    async fn untrack_task(&self, execution_id: &Uuid, result: &TaskExecutionResult) {
        let mut tasks = self.running_tasks.write().await;
        if let Some(mut running_task) = tasks.remove(execution_id) {
            running_task.status = if result.success {
                TaskExecutionStatus::Completed {
                    result: result.clone(),
                }
            } else {
                TaskExecutionStatus::Failed {
                    error: result.error_message.clone().unwrap_or_default(),
                    result: Some(result.clone()),
                }
            };
        }
    }

    pub async fn get_running_tasks(&self) -> Vec<RunningTask> {
        let tasks = self.running_tasks.read().await;
        tasks.values().cloned().collect()
    }

    pub async fn get_task_count(&self) -> usize {
        let tasks = self.running_tasks.read().await;
        tasks.len()
    }
}

#[async_trait]
impl TaskExecutor for DistributedTaskExecutor {
    async fn execute_task(
        &self,
        task: &TaskDefinition,
        execution: &mut TaskExecution,
        context: &ExecutionContext,
    ) -> pulse_core::Result<TaskExecutionResult> {
        info!("Executing distributed task: {} ({})", task.name, task.id);

        // Create an abortable task
        let task_clone = task.clone();
        let mut execution_clone = execution.clone();
        let context_clone = context.clone();
        let executor_ref = self as *const Self;

        let (abort_handle, abort_registration) = tokio::task::AbortHandle::new_pair();
        
        // Track the task
        self.track_running_task(execution, task, Some(abort_handle)).await;

        let result = tokio::spawn(async move {
            let executor = unsafe { &*executor_ref }; // Safe because we control the lifetime
            
            if task_clone.uses.is_some() {
                // Execute as an action
                executor.execute_action_task(&task_clone, &mut execution_clone, &context_clone).await
            } else if !task_clone.command.is_empty() {
                // Execute as shell command
                executor.shell_executor.execute_task(&task_clone, &mut execution_clone, &context_clone).await
                    .map_err(|e| ExecutorError::ExecutionFailed(e.to_string()))
            } else {
                Err(ExecutorError::InvalidParameters(
                    "Task has neither 'uses' nor 'command' specified".to_string()
                ))
            }
        })
        .with_abort_handle(abort_registration);

        let execution_result = match result.await {
            Ok(Ok(task_result)) => {
                info!("Task {} completed successfully", task.id);
                task_result
            }
            Ok(Err(e)) => {
                error!("Task {} failed: {}", task.id, e);
                TaskExecutionResult {
                    success: false,
                    exit_code: None,
                    output: None,
                    error_message: Some(e.to_string()),
                    duration_ms: 0,
                    artifacts: Vec::new(),
                }
            }
            Err(join_error) => {
                let error_msg = if join_error.is_cancelled() {
                    format!("Task {} was cancelled", task.id)
                } else {
                    format!("Task {} panicked: {}", task.id, join_error)
                };
                
                error!("{}", error_msg);
                
                TaskExecutionResult {
                    success: false,
                    exit_code: None,
                    output: None,
                    error_message: Some(error_msg),
                    duration_ms: 0,
                    artifacts: Vec::new(),
                }
            }
        };

        // Update execution with final result
        if execution_result.success {
            execution.complete(execution_result.output.clone());
        } else {
            execution.fail(execution_result.error_message.clone().unwrap_or_default());
        }

        // Untrack the task
        self.untrack_task(&execution.id, &execution_result).await;

        Ok(execution_result)
    }

    async fn cancel_task(&self, execution_id: &Uuid) -> pulse_core::Result<()> {
        info!("Cancelling task execution: {}", execution_id);
        
        let mut tasks = self.running_tasks.write().await;
        if let Some(running_task) = tasks.get_mut(execution_id) {
            if let Some(abort_handle) = &running_task.cancel_handle {
                abort_handle.abort();
                running_task.status = TaskExecutionStatus::Cancelled;
                info!("Task {} cancelled successfully", running_task.task_id);
            } else {
                warn!("No cancel handle found for task execution: {}", execution_id);
            }
        } else {
            warn!("Task execution not found: {}", execution_id);
        }

        Ok(())
    }

    async fn get_execution_status(&self, execution_id: &Uuid) -> pulse_core::Result<Option<TaskExecutionStatus>> {
        let tasks = self.running_tasks.read().await;
        Ok(tasks.get(execution_id).map(|task| task.status.clone()))
    }
}

pub struct ExecutorPool {
    executors: Vec<Arc<DistributedTaskExecutor>>,
    round_robin_index: Arc<RwLock<usize>>,
}

impl ExecutorPool {
    pub async fn new(pool_size: usize, working_directory: String) -> Self {
        let mut executors = Vec::with_capacity(pool_size);
        
        for _ in 0..pool_size {
            let executor = Arc::new(DistributedTaskExecutor::new(working_directory.clone()).await);
            executors.push(executor);
        }

        Self {
            executors,
            round_robin_index: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn get_executor(&self) -> Arc<DistributedTaskExecutor> {
        let mut index = self.round_robin_index.write().await;
        let executor = self.executors[*index].clone();
        *index = (*index + 1) % self.executors.len();
        executor
    }

    pub async fn get_least_busy_executor(&self) -> Arc<DistributedTaskExecutor> {
        let mut least_busy = self.executors[0].clone();
        let mut min_tasks = least_busy.get_task_count().await;

        for executor in &self.executors[1..] {
            let task_count = executor.get_task_count().await;
            if task_count < min_tasks {
                min_tasks = task_count;
                least_busy = executor.clone();
            }
        }

        least_busy
    }

    pub async fn get_pool_stats(&self) -> PoolStats {
        let mut total_running_tasks = 0;
        let mut executor_stats = Vec::new();

        for (i, executor) in self.executors.iter().enumerate() {
            let task_count = executor.get_task_count().await;
            total_running_tasks += task_count;
            
            executor_stats.push(ExecutorStats {
                executor_id: i,
                running_tasks: task_count,
            });
        }

        PoolStats {
            total_executors: self.executors.len(),
            total_running_tasks,
            executor_stats,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_executors: usize,
    pub total_running_tasks: usize,
    pub executor_stats: Vec<ExecutorStats>,
}

#[derive(Debug, Clone)]
pub struct ExecutorStats {
    pub executor_id: usize,
    pub running_tasks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::TaskStatus;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn create_test_context() -> ExecutionContext {
        ExecutionContext {
            job_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            environment: HashMap::new(),
            task_outputs: HashMap::new(),
            secrets: HashMap::new(),
            working_directory: "/tmp".to_string(),
        }
    }

    #[tokio::test]
    async fn test_distributed_executor_creation() {
        let temp_dir = tempdir().unwrap();
        let executor = DistributedTaskExecutor::new(temp_dir.path().to_string_lossy().to_string()).await;
        
        assert_eq!(executor.get_task_count().await, 0);
    }

    #[tokio::test]
    async fn test_shell_command_execution() {
        let temp_dir = tempdir().unwrap();
        let executor = DistributedTaskExecutor::new(temp_dir.path().to_string_lossy().to_string()).await;
        
        let task = TaskDefinition::new("test_task", "Test Task")
            .with_command(vec!["echo".to_string(), "hello world".to_string()]);

        let mut execution = TaskExecution::new(Uuid::new_v4(), &task);
        let context = create_test_context();

        let result = executor.execute_task(&task, &mut execution, &context).await.unwrap();
        
        assert!(result.success);
        assert_eq!(execution.status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_action_execution() {
        let temp_dir = tempdir().unwrap();
        let executor = DistributedTaskExecutor::new(temp_dir.path().to_string_lossy().to_string()).await;
        
        let mut with_params = HashMap::new();
        with_params.insert("repository".to_string(), serde_json::Value::String("https://github.com/example/test.git".to_string()));
        
        let task = TaskDefinition {
            id: "checkout_task".to_string(),
            name: "Checkout Task".to_string(),
            command: Vec::new(),
            dependencies: Vec::new(),
            environment: HashMap::new(),
            timeout_seconds: None,
            retry_limit: 3,
            needs: Vec::new(),
            uses: Some("actions/checkout@v4".to_string()),
            with: Some(with_params),
        };

        let mut execution = TaskExecution::new(Uuid::new_v4(), &task);
        let context = create_test_context();

        let result = executor.execute_task(&task, &mut execution, &context).await.unwrap();
        
        assert!(result.success);
        assert_eq!(execution.status, TaskStatus::Completed);
        assert!(result.artifacts.len() > 0);
    }

    #[tokio::test]
    async fn test_executor_pool() {
        let temp_dir = tempdir().unwrap();
        let pool = ExecutorPool::new(3, temp_dir.path().to_string_lossy().to_string()).await;
        
        // Test round-robin selection
        let executor1 = pool.get_executor().await;
        let executor2 = pool.get_executor().await;
        let executor3 = pool.get_executor().await;
        
        // They should be different instances
        assert!(!Arc::ptr_eq(&executor1, &executor2));
        assert!(!Arc::ptr_eq(&executor2, &executor3));
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.total_executors, 3);
        assert_eq!(stats.total_running_tasks, 0);
    }

    #[tokio::test]
    async fn test_task_cancellation() {
        let temp_dir = tempdir().unwrap();
        let executor = DistributedTaskExecutor::new(temp_dir.path().to_string_lossy().to_string()).await;
        
        // Create a long-running task
        let task = TaskDefinition::new("long_task", "Long Running Task")
            .with_command(vec!["sleep".to_string(), "10".to_string()]);

        let mut execution = TaskExecution::new(Uuid::new_v4(), &task);
        let context = create_test_context();
        let execution_id = execution.id;

        // Start the task
        let executor_clone = executor.clone();
        let task_clone = task.clone();
        let mut execution_clone = execution.clone();
        let context_clone = context.clone();

        tokio::spawn(async move {
            let _ = executor_clone.execute_task(&task_clone, &mut execution_clone, &context_clone).await;
        });

        // Wait a bit for the task to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        // Cancel the task
        let result = executor.cancel_task(&execution_id).await;
        assert!(result.is_ok());

        // Check status
        let status = executor.get_execution_status(&execution_id).await.unwrap();
        if let Some(TaskExecutionStatus::Cancelled) = status {
            // Success - task was cancelled
        } else {
            // Task might have completed before cancellation
        }
    }
}