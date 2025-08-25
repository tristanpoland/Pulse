use async_trait::async_trait;
use pulse_core::{
    ActionExecutor, ExecutionArtifact, ExecutionContext, ResourceLimits, TaskDefinition,
    TaskExecution, TaskExecutionResult, TaskExecutor, TaskStatus,
};
use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::{ExecutorError, Result};

pub struct ShellExecutor {
    resource_limits: ResourceLimits,
    working_directory: String,
}

impl ShellExecutor {
    pub fn new(working_directory: String) -> Self {
        Self {
            resource_limits: ResourceLimits::default(),
            working_directory,
        }
    }

    pub fn with_resource_limits(mut self, limits: ResourceLimits) -> Self {
        self.resource_limits = limits;
        self
    }

    async fn execute_command(
        &self,
        command: &[String],
        context: &ExecutionContext,
        task_timeout: Option<Duration>,
    ) -> Result<TaskExecutionResult> {
        if command.is_empty() {
            return Err(ExecutorError::InvalidParameters("Empty command".to_string()));
        }

        let start_time = Instant::now();
        
        info!("Executing command: {:?}", command);

        let mut cmd = Command::new(&command[0]);
        cmd.args(&command[1..])
            .current_dir(&self.working_directory)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null());

        // Set environment variables
        for (key, value) in &context.environment {
            cmd.env(key, value);
        }

        // Apply resource limits (simplified)
        if let Some(timeout_seconds) = self.resource_limits.max_execution_time_seconds {
            // This will be handled by the timeout wrapper
            debug!("Command timeout set to {} seconds", timeout_seconds);
        }

        let execution_timeout = task_timeout
            .or_else(|| {
                self.resource_limits
                    .max_execution_time_seconds
                    .map(Duration::from_secs)
            })
            .unwrap_or(Duration::from_secs(3600)); // Default 1 hour

        // Execute with timeout
        let result = timeout(execution_timeout, async {
            let mut child = cmd.spawn()
                .map_err(|e| ExecutorError::ExecutionFailed(format!("Failed to spawn process: {}", e)))?;

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let stdout_reader = BufReader::new(stdout);
            let stderr_reader = BufReader::new(stderr);

            let mut stdout_lines = stdout_reader.lines();
            let mut stderr_lines = stderr_reader.lines();

            let mut stdout_output = Vec::new();
            let mut stderr_output = Vec::new();

            // Read output streams concurrently
            tokio::spawn(async move {
                while let Ok(Some(line)) = stdout_lines.next_line().await {
                    debug!("STDOUT: {}", line);
                    stdout_output.push(line);
                }
                stdout_output
            });

            tokio::spawn(async move {
                while let Ok(Some(line)) = stderr_lines.next_line().await {
                    debug!("STDERR: {}", line);
                    stderr_output.push(line);
                }
                stderr_output
            });

            let status = child.wait().await
                .map_err(|e| ExecutorError::ExecutionFailed(format!("Failed to wait for process: {}", e)))?;

            Ok::<(std::process::ExitStatus, Vec<String>, Vec<String>), ExecutorError>((status, stdout_output, stderr_output))
        }).await;

        let duration = start_time.elapsed();

        match result {
            Ok(Ok((status, stdout_output, stderr_output))) => {
                let exit_code = status.code();
                let success = status.success();

                let mut output_data = HashMap::new();
                output_data.insert("stdout".to_string(), serde_json::Value::Array(
                    stdout_output.into_iter().map(serde_json::Value::String).collect()
                ));
                output_data.insert("stderr".to_string(), serde_json::Value::Array(
                    stderr_output.into_iter().map(serde_json::Value::String).collect()
                ));

                let result = TaskExecutionResult {
                    success,
                    exit_code,
                    output: Some(output_data.into_iter().collect()),
                    error_message: if !success {
                        Some(format!("Command exited with code: {:?}", exit_code))
                    } else {
                        None
                    },
                    duration_ms: duration.as_millis() as u64,
                    artifacts: Vec::new(), // TODO: Implement artifact collection
                };

                info!("Command completed in {}ms with exit code: {:?}", result.duration_ms, exit_code);
                Ok(result)
            }
            Ok(Err(e)) => {
                error!("Command execution error: {}", e);
                Err(e)
            }
            Err(_) => {
                let timeout_seconds = execution_timeout.as_secs();
                warn!("Command timed out after {} seconds", timeout_seconds);
                Err(ExecutorError::TimeoutExceeded { timeout_seconds })
            }
        }
    }
}

#[async_trait]
impl TaskExecutor for ShellExecutor {
    async fn execute_task(
        &self,
        task: &TaskDefinition,
        execution: &mut TaskExecution,
        context: &ExecutionContext,
    ) -> pulse_core::Result<TaskExecutionResult> {
        debug!("Executing shell task: {}", task.id);

        execution.start(context.worker_id);

        let timeout = task.timeout_seconds.map(Duration::from_secs);
        
        match self.execute_command(&task.command, context, timeout).await {
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
                
                let result = TaskExecutionResult {
                    success: false,
                    exit_code: None,
                    output: None,
                    error_message: Some(error_msg),
                    duration_ms: 0,
                    artifacts: Vec::new(),
                };
                
                Ok(result)
            }
        }
    }

    async fn cancel_task(&self, execution_id: &Uuid) -> pulse_core::Result<()> {
        // In a real implementation, we'd track running processes and kill them
        warn!("Task cancellation not fully implemented for execution: {}", execution_id);
        Ok(())
    }

    async fn get_execution_status(&self, execution_id: &Uuid) -> pulse_core::Result<Option<pulse_core::TaskExecutionStatus>> {
        // In a real implementation, we'd track execution status
        debug!("Status check not implemented for execution: {}", execution_id);
        Ok(None)
    }
}

pub struct ShellActionExecutor;

impl ShellActionExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ShellActionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ActionExecutor for ShellActionExecutor {
    fn can_execute(&self, action: &str) -> bool {
        matches!(action, "shell" | "run" | "bash" | "cmd")
    }

    async fn execute_action(
        &self,
        action: &str,
        parameters: &HashMap<String, serde_json::Value>,
        context: &ExecutionContext,
    ) -> pulse_core::Result<TaskExecutionResult> {
        if !self.can_execute(action) {
            return Err(ExecutorError::ActionNotFound { 
                action: action.to_string() 
            }.into());
        }

        // Extract command from parameters
        let command = match parameters.get("run").or_else(|| parameters.get("command")) {
            Some(serde_json::Value::String(cmd)) => cmd.clone(),
            Some(serde_json::Value::Array(cmd_array)) => {
                cmd_array.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            }
            _ => {
                return Err(ExecutorError::InvalidParameters(
                    "Missing 'run' or 'command' parameter".to_string()
                ).into());
            }
        };

        // Create shell command
        let shell_cmd = if cfg!(target_os = "windows") {
            vec!["cmd".to_string(), "/C".to_string(), command]
        } else {
            vec!["sh".to_string(), "-c".to_string(), command]
        };

        // Create a task definition for execution
        let task_def = TaskDefinition::new("shell_action", "Shell Action")
            .with_command(shell_cmd);

        let mut execution = TaskExecution::new(context.job_id, &task_def);

        // Use shell executor
        let executor = ShellExecutor::new(context.working_directory.clone());
        executor.execute_task(&task_def, &mut execution, context).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn test_shell_executor_simple_command() {
        let temp_dir = tempdir().unwrap();
        let executor = ShellExecutor::new(temp_dir.path().to_string_lossy().to_string());

        let command = if cfg!(target_os = "windows") {
            vec!["echo".to_string(), "hello".to_string()]
        } else {
            vec!["echo".to_string(), "hello".to_string()]
        };

        let context = create_test_context();
        let result = executor.execute_command(&command, &context, None).await.unwrap();

        assert!(result.success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.output.is_some());
    }

    #[tokio::test]
    async fn test_shell_executor_with_timeout() {
        let temp_dir = tempdir().unwrap();
        let executor = ShellExecutor::new(temp_dir.path().to_string_lossy().to_string());

        let command = if cfg!(target_os = "windows") {
            vec!["timeout".to_string(), "5".to_string()]
        } else {
            vec!["sleep".to_string(), "5".to_string()]
        };

        let context = create_test_context();
        let timeout = Some(Duration::from_secs(1)); // 1 second timeout

        let result = executor.execute_command(&command, &context, timeout).await;
        assert!(result.is_err());
        
        if let Err(ExecutorError::TimeoutExceeded { timeout_seconds }) = result {
            assert_eq!(timeout_seconds, 1);
        } else {
            panic!("Expected timeout error");
        }
    }

    #[tokio::test]
    async fn test_shell_action_executor() {
        let executor = ShellActionExecutor::new();
        
        assert!(executor.can_execute("shell"));
        assert!(executor.can_execute("run"));
        assert!(!executor.can_execute("docker"));

        let mut parameters = HashMap::new();
        parameters.insert("run".to_string(), serde_json::Value::String("echo test".to_string()));

        let context = create_test_context();
        let result = executor.execute_action("shell", &parameters, &context).await.unwrap();

        assert!(result.success);
    }
}