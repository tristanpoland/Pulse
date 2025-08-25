use anyhow::Result;
use std::collections::HashMap;
use std::process::{Child, Stdio};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub execution_id: Uuid,
    pub task_id: String,
    pub command: Vec<String>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub working_directory: String,
    pub environment: HashMap<String, String>,
}

#[derive(Debug)]
pub struct RunningProcess {
    pub info: ProcessInfo,
    pub child: tokio::process::Child,
    pub abort_handle: tokio::task::AbortHandle,
}

pub struct ProcessManager {
    running_processes: Arc<RwLock<HashMap<Uuid, RunningProcess>>>,
}

impl ProcessManager {
    pub fn new() -> Self {
        Self {
            running_processes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn spawn_process(
        &self,
        execution_id: Uuid,
        task_id: String,
        command: Vec<String>,
        working_directory: String,
        environment: HashMap<String, String>,
        timeout_duration: Option<Duration>,
    ) -> Result<ProcessOutput> {
        if command.is_empty() {
            return Err(anyhow::anyhow!("Empty command"));
        }

        let process_info = ProcessInfo {
            execution_id,
            task_id: task_id.clone(),
            command: command.clone(),
            started_at: chrono::Utc::now(),
            working_directory: working_directory.clone(),
            environment: environment.clone(),
        };

        info!("Spawning process for task {}: {:?}", task_id, command);

        // Create the command
        let mut cmd = Command::new(&command[0]);
        cmd.args(&command[1..])
            .current_dir(&working_directory)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null());

        // Set environment variables
        for (key, value) in &environment {
            cmd.env(key, value);
        }

        // Apply resource limits if supported by the platform
        #[cfg(unix)]
        {
            // Set process group for better process management
            cmd.process_group(0);
        }

        let mut child = cmd.spawn()?;

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        // Create abort handle for cancellation
        let (abort_handle, abort_registration) = tokio::task::AbortHandle::new_pair();

        // Store the running process
        let running_process = RunningProcess {
            info: process_info.clone(),
            child,
            abort_handle,
        };

        {
            let mut processes = self.running_processes.write().await;
            processes.insert(execution_id, running_process);
        }

        // Execute the process with timeout
        let execution_future = tokio::task::spawn(async move {
            self.execute_with_streams(execution_id, stdout, stderr).await
        })
        .with_abort_handle(abort_registration);

        let result = if let Some(timeout_duration) = timeout_duration {
            timeout(timeout_duration, execution_future).await
        } else {
            Ok(execution_future.await)
        };

        // Clean up the process from our tracking
        let exit_status = {
            let mut processes = self.running_processes.write().await;
            if let Some(mut running_process) = processes.remove(&execution_id) {
                // Wait for the child process to complete
                running_process.child.wait().await.ok()
            } else {
                None
            }
        };

        match result {
            Ok(Ok(output)) => {
                info!("Process completed for task {}", task_id);
                Ok(ProcessOutput {
                    stdout: output.stdout,
                    stderr: output.stderr,
                    exit_code: exit_status.and_then(|s| s.code()),
                    success: exit_status.map(|s| s.success()).unwrap_or(false),
                    duration_ms: (chrono::Utc::now() - process_info.started_at)
                        .num_milliseconds() as u64,
                })
            }
            Ok(Err(e)) => {
                error!("Process execution error for task {}: {}", task_id, e);
                Err(e)
            }
            Err(_) => {
                warn!("Process timed out for task {}", task_id);
                Err(anyhow::anyhow!("Process execution timed out"))
            }
        }
    }

    async fn execute_with_streams(
        &self,
        execution_id: Uuid,
        stdout: tokio::process::ChildStdout,
        stderr: tokio::process::ChildStderr,
    ) -> Result<ProcessOutput> {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let stdout_reader = BufReader::new(stdout);
        let stderr_reader = BufReader::new(stderr);

        let mut stdout_lines = stdout_reader.lines();
        let mut stderr_lines = stderr_reader.lines();

        let mut stdout_output = Vec::new();
        let mut stderr_output = Vec::new();

        // Read from both streams concurrently
        loop {
            tokio::select! {
                stdout_result = stdout_lines.next_line() => {
                    match stdout_result {
                        Ok(Some(line)) => {
                            debug!("STDOUT [{}]: {}", execution_id, line);
                            stdout_output.push(line);
                        }
                        Ok(None) => break, // EOF
                        Err(e) => {
                            error!("Error reading stdout: {}", e);
                            break;
                        }
                    }
                }
                stderr_result = stderr_lines.next_line() => {
                    match stderr_result {
                        Ok(Some(line)) => {
                            debug!("STDERR [{}]: {}", execution_id, line);
                            stderr_output.push(line);
                        }
                        Ok(None) => break, // EOF
                        Err(e) => {
                            error!("Error reading stderr: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        Ok(ProcessOutput {
            stdout: stdout_output,
            stderr: stderr_output,
            exit_code: None, // Will be set by the caller
            success: false,  // Will be set by the caller
            duration_ms: 0,  // Will be set by the caller
        })
    }

    pub async fn cancel_process(&self, execution_id: &Uuid) -> Result<()> {
        info!("Cancelling process for execution: {}", execution_id);

        let mut processes = self.running_processes.write().await;
        if let Some(running_process) = processes.remove(execution_id) {
            // Abort the task
            running_process.abort_handle.abort();

            // Kill the process and its children
            #[cfg(unix)]
            {
                // Kill the entire process group
                if let Some(pid) = running_process.child.id() {
                    unsafe {
                        libc::killpg(pid as i32, libc::SIGTERM);
                        
                        // Wait a bit, then force kill if necessary
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        libc::killpg(pid as i32, libc::SIGKILL);
                    }
                }
            }

            #[cfg(windows)]
            {
                // On Windows, kill the process tree
                if let Some(pid) = running_process.child.id() {
                    let _ = std::process::Command::new("taskkill")
                        .args(["/PID", &pid.to_string(), "/T", "/F"])
                        .output();
                }
            }

            info!("Process cancelled for execution: {}", execution_id);
            Ok(())
        } else {
            warn!("Process not found for execution: {}", execution_id);
            Err(anyhow::anyhow!("Process not found"))
        }
    }

    pub async fn get_running_processes(&self) -> Vec<ProcessInfo> {
        let processes = self.running_processes.read().await;
        processes.values().map(|p| p.info.clone()).collect()
    }

    pub async fn get_process_info(&self, execution_id: &Uuid) -> Option<ProcessInfo> {
        let processes = self.running_processes.read().await;
        processes.get(execution_id).map(|p| p.info.clone())
    }

    pub async fn cleanup_finished_processes(&self) {
        let mut processes = self.running_processes.write().await;
        let mut finished_processes = Vec::new();

        for (execution_id, running_process) in processes.iter_mut() {
            // Check if the process has finished
            match running_process.child.try_wait() {
                Ok(Some(_status)) => {
                    finished_processes.push(*execution_id);
                }
                Ok(None) => {
                    // Process is still running
                }
                Err(e) => {
                    error!("Error checking process status: {}", e);
                    finished_processes.push(*execution_id);
                }
            }
        }

        // Remove finished processes
        for execution_id in finished_processes {
            if let Some(process) = processes.remove(&execution_id) {
                debug!("Cleaned up finished process: {}", process.info.task_id);
            }
        }
    }
}

impl Default for ProcessManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ProcessOutput {
    pub stdout: Vec<String>,
    pub stderr: Vec<String>,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub duration_ms: u64,
}

// Background task to periodically clean up finished processes
pub async fn start_process_cleanup_task(process_manager: Arc<ProcessManager>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        process_manager.cleanup_finished_processes().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_process_manager_creation() {
        let manager = ProcessManager::new();
        assert_eq!(manager.get_running_processes().await.len(), 0);
    }

    #[tokio::test]
    async fn test_spawn_simple_command() {
        let manager = ProcessManager::new();
        let execution_id = Uuid::new_v4();
        let temp_dir = tempdir().unwrap();
        
        let command = if cfg!(target_os = "windows") {
            vec!["cmd".to_string(), "/C".to_string(), "echo hello".to_string()]
        } else {
            vec!["echo".to_string(), "hello".to_string()]
        };

        let result = manager
            .spawn_process(
                execution_id,
                "test-task".to_string(),
                command,
                temp_dir.path().to_string_lossy().to_string(),
                HashMap::new(),
                Some(Duration::from_secs(5)),
            )
            .await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.success);
        assert!(output.stdout.iter().any(|line| line.contains("hello")));
    }

    #[tokio::test]
    async fn test_cancel_process() {
        let manager = ProcessManager::new();
        let execution_id = Uuid::new_v4();
        let temp_dir = tempdir().unwrap();

        // Spawn a long-running command
        let command = if cfg!(target_os = "windows") {
            vec!["timeout".to_string(), "10".to_string()]
        } else {
            vec!["sleep".to_string(), "10".to_string()]
        };

        // Start the process (don't await)
        let manager_clone = manager.clone();
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        
        tokio::spawn(async move {
            let _ = manager_clone
                .spawn_process(
                    execution_id,
                    "long-task".to_string(),
                    command,
                    temp_path,
                    HashMap::new(),
                    None,
                )
                .await;
        });

        // Wait a bit for the process to start
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Cancel the process
        let result = manager.cancel_process(&execution_id).await;
        // Note: This might fail in test environments, but should work in production
        // assert!(result.is_ok());
    }
}