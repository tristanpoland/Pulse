use async_trait::async_trait;
use pulse_core::{
    ActionExecutor, ExecutionArtifact, ExecutionContext, TaskExecutionResult,
};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::error::{ExecutorError, Result};

pub struct DockerActionExecutor {
    docker_available: bool,
}

impl DockerActionExecutor {
    pub async fn new() -> Self {
        let docker_available = Self::check_docker_availability().await;
        if !docker_available {
            warn!("Docker is not available on this system");
        }
        
        Self { docker_available }
    }

    async fn check_docker_availability() -> bool {
        match tokio::process::Command::new("docker")
            .arg("version")
            .output()
            .await
        {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    async fn execute_docker_command(
        &self,
        image: &str,
        command: Option<&str>,
        parameters: &HashMap<String, serde_json::Value>,
        context: &ExecutionContext,
    ) -> Result<TaskExecutionResult> {
        if !self.docker_available {
            return Err(ExecutorError::DockerError(
                "Docker is not available on this system".to_string()
            ));
        }

        let start_time = Instant::now();
        
        info!("Executing Docker container: {}", image);

        let mut cmd = tokio::process::Command::new("docker");
        cmd.arg("run")
            .arg("--rm") // Remove container after execution
            .arg("-i"); // Interactive mode

        // Add environment variables
        for (key, value) in &context.environment {
            cmd.arg("-e").arg(format!("{}={}", key, value));
        }

        // Add working directory mount if specified
        if !context.working_directory.is_empty() {
            cmd.arg("-v")
                .arg(format!("{}:/workspace", context.working_directory))
                .arg("-w")
                .arg("/workspace");
        }

        // Add volumes from parameters
        if let Some(volumes) = parameters.get("volumes") {
            if let serde_json::Value::Array(volume_array) = volumes {
                for volume in volume_array {
                    if let serde_json::Value::String(vol_str) = volume {
                        cmd.arg("-v").arg(vol_str);
                    }
                }
            } else if let serde_json::Value::String(vol_str) = volumes {
                cmd.arg("-v").arg(vol_str);
            }
        }

        // Add ports from parameters
        if let Some(ports) = parameters.get("ports") {
            if let serde_json::Value::Array(port_array) = ports {
                for port in port_array {
                    if let serde_json::Value::String(port_str) = port {
                        cmd.arg("-p").arg(port_str);
                    }
                }
            } else if let serde_json::Value::String(port_str) = ports {
                cmd.arg("-p").arg(port_str);
            }
        }

        // Add resource limits
        if let Some(memory) = parameters.get("memory") {
            if let serde_json::Value::String(mem_str) = memory {
                cmd.arg("--memory").arg(mem_str);
            }
        }

        if let Some(cpus) = parameters.get("cpus") {
            if let serde_json::Value::String(cpu_str) = cpus {
                cmd.arg("--cpus").arg(cpu_str);
            } else if let serde_json::Value::Number(cpu_num) = cpus {
                cmd.arg("--cpus").arg(cpu_num.to_string());
            }
        }

        // Add the image
        cmd.arg(image);

        // Add command if provided
        if let Some(cmd_str) = command {
            // Split command into args (simplified)
            for arg in cmd_str.split_whitespace() {
                cmd.arg(arg);
            }
        }

        debug!("Docker command: {:?}", cmd);

        // Execute the command
        match cmd.output().await {
            Ok(output) => {
                let duration = start_time.elapsed();
                let exit_code = output.status.code();
                let success = output.status.success();

                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();

                let mut output_data = HashMap::new();
                output_data.insert("stdout".to_string(), serde_json::Value::String(stdout));
                output_data.insert("stderr".to_string(), serde_json::Value::String(stderr));

                let result = TaskExecutionResult {
                    success,
                    exit_code,
                    output: Some(output_data.into_iter().collect()),
                    error_message: if !success {
                        Some(format!("Docker container exited with code: {:?}", exit_code))
                    } else {
                        None
                    },
                    duration_ms: duration.as_millis() as u64,
                    artifacts: Vec::new(),
                };

                info!("Docker container completed in {}ms with exit code: {:?}", result.duration_ms, exit_code);
                Ok(result)
            }
            Err(e) => {
                error!("Failed to execute docker command: {}", e);
                Err(ExecutorError::DockerError(format!("Docker execution failed: {}", e)))
            }
        }
    }
}

impl Default for DockerActionExecutor {
    fn default() -> Self {
        Self {
            docker_available: false, // Will be checked when new() is called
        }
    }
}

#[async_trait]
impl ActionExecutor for DockerActionExecutor {
    fn can_execute(&self, action: &str) -> bool {
        self.docker_available && (
            action.starts_with("docker://") || 
            action == "docker" ||
            action.starts_with("ghcr.io/") ||
            action.starts_with("gcr.io/") ||
            action.starts_with("docker.io/")
        )
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

        // Parse the action to get the Docker image
        let image = if action.starts_with("docker://") {
            &action[9..] // Remove "docker://" prefix
        } else if action == "docker" {
            // Get image from parameters
            match parameters.get("image") {
                Some(serde_json::Value::String(img)) => img,
                _ => {
                    return Err(ExecutorError::InvalidParameters(
                        "Missing 'image' parameter for docker action".to_string()
                    ).into());
                }
            }
        } else {
            action // Assume it's a direct image reference
        };

        // Get command from parameters
        let command = parameters.get("run")
            .or_else(|| parameters.get("cmd"))
            .or_else(|| parameters.get("command"))
            .and_then(|v| v.as_str());

        debug!("Executing Docker action with image: {} and command: {:?}", image, command);

        self.execute_docker_command(image, command, parameters, context)
            .await
            .map_err(Into::into)
    }
}

// Built-in Docker actions
pub struct CheckoutActionExecutor;

impl CheckoutActionExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CheckoutActionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ActionExecutor for CheckoutActionExecutor {
    fn can_execute(&self, action: &str) -> bool {
        action == "actions/checkout" || 
        action.starts_with("actions/checkout@")
    }

    async fn execute_action(
        &self,
        action: &str,
        parameters: &HashMap<String, serde_json::Value>,
        context: &ExecutionContext,
    ) -> pulse_core::Result<TaskExecutionResult> {
        info!("Executing checkout action: {}", action);

        // In a real implementation, this would clone a git repository
        // For now, we'll simulate it
        
        let start_time = Instant::now();
        
        // Extract repository URL from parameters
        let repository = parameters.get("repository")
            .and_then(|v| v.as_str())
            .unwrap_or("https://github.com/example/repo.git");

        let ref_name = parameters.get("ref")
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        info!("Checking out repository: {} at ref: {}", repository, ref_name);

        // Simulate git clone
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let duration = start_time.elapsed();

        let mut output_data = HashMap::new();
        output_data.insert("repository".to_string(), serde_json::Value::String(repository.to_string()));
        output_data.insert("ref".to_string(), serde_json::Value::String(ref_name.to_string()));
        output_data.insert("path".to_string(), serde_json::Value::String(context.working_directory.clone()));

        let result = TaskExecutionResult {
            success: true,
            exit_code: Some(0),
            output: Some(output_data.into_iter().collect()),
            error_message: None,
            duration_ms: duration.as_millis() as u64,
            artifacts: vec![
                ExecutionArtifact {
                    name: "repository".to_string(),
                    path: context.working_directory.clone(),
                    size_bytes: 1024, // Simulated
                    content_type: Some("application/git".to_string()),
                }.into()
            ],
        };

        info!("Checkout completed in {}ms", result.duration_ms);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use uuid::Uuid;

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
    async fn test_docker_action_executor_creation() {
        let executor = DockerActionExecutor::new().await;
        
        // Test basic capability checking
        if executor.docker_available {
            assert!(executor.can_execute("docker://ubuntu:latest"));
            assert!(executor.can_execute("docker"));
        } else {
            assert!(!executor.can_execute("docker://ubuntu:latest"));
        }
    }

    #[tokio::test]
    async fn test_checkout_action_executor() {
        let executor = CheckoutActionExecutor::new();
        
        assert!(executor.can_execute("actions/checkout"));
        assert!(executor.can_execute("actions/checkout@v4"));
        assert!(!executor.can_execute("actions/setup-node"));

        let mut parameters = HashMap::new();
        parameters.insert("repository".to_string(), serde_json::Value::String("https://github.com/example/test.git".to_string()));
        parameters.insert("ref".to_string(), serde_json::Value::String("main".to_string()));

        let context = create_test_context();
        let result = executor.execute_action("actions/checkout@v4", &parameters, &context).await.unwrap();

        assert!(result.success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.output.is_some());
        assert_eq!(result.artifacts.len(), 1);
    }

    #[test]
    fn test_docker_action_parsing() {
        let executor = DockerActionExecutor::default();
        
        // Test different action formats
        let test_cases = vec![
            ("docker://ubuntu:latest", true),
            ("docker", false), // Would be true if docker_available was true
            ("ghcr.io/actions/runner:latest", false), // Would be true if docker_available was true
            ("actions/checkout", false),
        ];

        for (action, expected) in test_cases {
            assert_eq!(executor.can_execute(action), expected, "Failed for action: {}", action);
        }
    }
}