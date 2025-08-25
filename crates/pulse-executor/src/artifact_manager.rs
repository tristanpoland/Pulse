use pulse_core::{Artifact, ArtifactType, TaskExecution, ExecutionContext};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::{ExecutorError, Result};

/// Manages artifacts and data passing between tasks
pub struct ArtifactManager {
    artifacts_dir: PathBuf,
    task_artifacts: Arc<RwLock<HashMap<Uuid, Vec<Artifact>>>>,
    task_outputs: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl ArtifactManager {
    pub async fn new(artifacts_dir: PathBuf) -> Result<Self> {
        // Ensure artifacts directory exists
        if !artifacts_dir.exists() {
            fs::create_dir_all(&artifacts_dir)
                .await?;
        }

        info!("Initialized artifact manager with directory: {}", artifacts_dir.display());

        Ok(Self {
            artifacts_dir,
            task_artifacts: Arc::new(RwLock::new(HashMap::new())),
            task_outputs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Collect artifacts from a task execution
    pub async fn collect_artifacts(
        &self,
        execution: &TaskExecution,
        context: &ExecutionContext,
        artifact_patterns: &[String],
    ) -> Result<Vec<Artifact>> {
        let working_dir = Path::new(&context.working_directory);
        let mut collected_artifacts = Vec::new();

        info!("Collecting artifacts for task {} with {} patterns", 
              execution.task_id, artifact_patterns.len());

        for pattern in artifact_patterns {
            debug!("Processing artifact pattern: {}", pattern);
            
            let artifacts = self.collect_artifacts_by_pattern(
                execution, 
                working_dir, 
                pattern
            ).await?;
            
            collected_artifacts.extend(artifacts);
        }

        // Store artifacts in our registry
        let mut task_artifacts = self.task_artifacts.write().await;
        task_artifacts.insert(execution.id, collected_artifacts.clone());

        info!("Collected {} artifacts for task {}", 
              collected_artifacts.len(), execution.task_id);

        Ok(collected_artifacts)
    }

    async fn collect_artifacts_by_pattern(
        &self,
        execution: &TaskExecution,
        working_dir: &Path,
        pattern: &str,
    ) -> Result<Vec<Artifact>> {
        let mut artifacts = Vec::new();

        // Handle different pattern types
        if pattern.starts_with("file:") {
            // Single file artifact
            let file_path = pattern.strip_prefix("file:").unwrap();
            let full_path = working_dir.join(file_path);
            
            if full_path.exists() {
                let artifact = self.create_file_artifact(execution, &full_path, file_path).await?;
                artifacts.push(artifact);
            } else {
                warn!("Artifact file not found: {}", full_path.display());
            }
        } else if pattern.starts_with("glob:") {
            // Glob pattern
            let glob_pattern = pattern.strip_prefix("glob:").unwrap();
            artifacts.extend(self.collect_glob_artifacts(execution, working_dir, glob_pattern).await?);
        } else if pattern.starts_with("dir:") {
            // Directory artifact
            let dir_path = pattern.strip_prefix("dir:").unwrap();
            let full_path = working_dir.join(dir_path);
            
            if full_path.is_dir() {
                let artifact = self.create_directory_artifact(execution, &full_path, dir_path).await?;
                artifacts.push(artifact);
            } else {
                warn!("Artifact directory not found: {}", full_path.display());
            }
        } else {
            // Default to file pattern
            let full_path = working_dir.join(pattern);
            
            if full_path.exists() {
                let artifact = if full_path.is_file() {
                    self.create_file_artifact(execution, &full_path, pattern).await?
                } else {
                    self.create_directory_artifact(execution, &full_path, pattern).await?
                };
                artifacts.push(artifact);
            } else {
                warn!("Artifact not found: {}", full_path.display());
            }
        }

        Ok(artifacts)
    }

    async fn collect_glob_artifacts(
        &self,
        execution: &TaskExecution,
        working_dir: &Path,
        glob_pattern: &str,
    ) -> Result<Vec<Artifact>> {
        use glob::glob;
        
        let mut artifacts = Vec::new();
        let full_pattern = working_dir.join(glob_pattern);
        let pattern_str = full_pattern.to_string_lossy();

        debug!("Collecting artifacts with glob pattern: {}", pattern_str);

        match glob(&pattern_str) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(path) => {
                            let relative_path = path.strip_prefix(working_dir)
                                .unwrap_or(&path)
                                .to_string_lossy();
                            
                            let artifact = if path.is_file() {
                                self.create_file_artifact(execution, &path, &relative_path).await?
                            } else if path.is_dir() {
                                self.create_directory_artifact(execution, &path, &relative_path).await?
                            } else {
                                continue;
                            };
                            
                            artifacts.push(artifact);
                        }
                        Err(e) => {
                            warn!("Error reading glob entry: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Invalid glob pattern '{}': {}", glob_pattern, e);
                return Err(ExecutorError::InvalidParameters(
                    format!("Invalid glob pattern: {}", e)
                ));
            }
        }

        Ok(artifacts)
    }

    async fn create_file_artifact(
        &self,
        execution: &TaskExecution,
        file_path: &Path,
        relative_path: &str,
    ) -> Result<Artifact> {
        let metadata = fs::metadata(file_path).await?;

        // Copy file to artifacts directory
        let artifact_file_name = format!("{}_{}", execution.id, relative_path.replace('/', "_"));
        let artifact_path = self.artifacts_dir.join(&artifact_file_name);

        // Ensure parent directory exists
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::copy(file_path, &artifact_path).await?;

        debug!("Created file artifact: {} -> {}", file_path.display(), artifact_path.display());

        Ok(Artifact {
            id: Uuid::new_v4(),
            name: relative_path.to_string(),
            artifact_type: ArtifactType::File,
            path: artifact_path.to_string_lossy().to_string(),
            size_bytes: metadata.len(),
            created_at: chrono::Utc::now(),
            task_execution_id: execution.id,
            metadata: HashMap::new(),
        })
    }

    async fn create_directory_artifact(
        &self,
        execution: &TaskExecution,
        dir_path: &Path,
        relative_path: &str,
    ) -> Result<Artifact> {
        // Create a compressed archive of the directory
        let artifact_file_name = format!("{}_{}.tar.gz", execution.id, relative_path.replace('/', "_"));
        let artifact_path = self.artifacts_dir.join(&artifact_file_name);

        let size_bytes = self.compress_directory(dir_path, &artifact_path).await?;

        debug!("Created directory artifact: {} -> {}", dir_path.display(), artifact_path.display());

        Ok(Artifact {
            id: Uuid::new_v4(),
            name: relative_path.to_string(),
            artifact_type: ArtifactType::Archive,
            path: artifact_path.to_string_lossy().to_string(),
            size_bytes,
            created_at: chrono::Utc::now(),
            task_execution_id: execution.id,
            metadata: HashMap::new(),
        })
    }

    async fn compress_directory(&self, dir_path: &Path, output_path: &Path) -> Result<u64> {
        use tokio::process::Command;

        let output = Command::new("tar")
            .args(&[
                "-czf",
                &output_path.to_string_lossy(),
                "-C",
                &dir_path.parent().unwrap_or(dir_path).to_string_lossy(),
                &dir_path.file_name().unwrap().to_string_lossy(),
            ])
            .output()
            .await
            .map_err(|e| ExecutorError::ExecutionFailed(format!("Failed to compress directory: {}", e)))?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            return Err(ExecutorError::ExecutionFailed(
                format!("Directory compression failed: {}", error)
            ));
        }

        let metadata = fs::metadata(output_path).await?;

        Ok(metadata.len())
    }

    /// Store task output for data passing between tasks
    pub async fn store_task_output(
        &self,
        task_id: &str,
        output_name: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", task_id, output_name);
        let mut task_outputs = self.task_outputs.write().await;
        task_outputs.insert(key.clone(), value.clone());

        debug!("Stored task output: {} = {:?}", key, value);
        Ok(())
    }

    /// Get task output for data passing
    pub async fn get_task_output(
        &self,
        task_id: &str,
        output_name: &str,
    ) -> Result<Option<serde_json::Value>> {
        let key = format!("{}:{}", task_id, output_name);
        let task_outputs = self.task_outputs.read().await;
        Ok(task_outputs.get(&key).cloned())
    }

    /// Resolve task dependencies and prepare context with outputs
    pub async fn prepare_execution_context(
        &self,
        base_context: ExecutionContext,
        task_dependencies: &[String],
    ) -> Result<ExecutionContext> {
        let mut context = base_context;
        
        // Collect outputs from dependent tasks
        for dep_task_id in task_dependencies {
            let task_outputs = self.task_outputs.read().await;
            
            // Find all outputs for this dependency
            let dep_outputs: HashMap<String, serde_json::Value> = task_outputs
                .iter()
                .filter(|(key, _)| key.starts_with(&format!("{}:", dep_task_id)))
                .map(|(key, value)| {
                    let output_name = key.strip_prefix(&format!("{}:", dep_task_id)).unwrap();
                    (output_name.to_string(), value.clone())
                })
                .collect();

            if !dep_outputs.is_empty() {
                context.task_outputs.insert(dep_task_id.clone(), dep_outputs);
                debug!("Added {} outputs from dependency task {}", 
                       context.task_outputs[dep_task_id].len(), dep_task_id);
            }
        }

        Ok(context)
    }

    /// Extract artifacts from a task directory after execution
    pub async fn extract_artifacts_to_working_dir(
        &self,
        artifact: &Artifact,
        target_dir: &Path,
    ) -> Result<()> {
        let artifact_path = Path::new(&artifact.path);
        
        match artifact.artifact_type {
            ArtifactType::File => {
                let target_path = target_dir.join(&artifact.name);
                
                // Ensure target directory exists
                if let Some(parent) = target_path.parent() {
                    fs::create_dir_all(parent).await?;
                }

                fs::copy(artifact_path, target_path).await?;
            }
            ArtifactType::Archive => {
                // Extract compressed archive
                use tokio::process::Command;

                let output = Command::new("tar")
                    .args(&[
                        "-xzf",
                        &artifact_path.to_string_lossy(),
                        "-C",
                        &target_dir.to_string_lossy(),
                    ])
                    .output()
                    .await
                    .map_err(|e| ExecutorError::ExecutionFailed(format!("Failed to extract archive: {}", e)))?;

                if !output.status.success() {
                    let error = String::from_utf8_lossy(&output.stderr);
                    return Err(ExecutorError::ExecutionFailed(
                        format!("Archive extraction failed: {}", error)
                    ));
                }
            }
            ArtifactType::Log => {
                // Copy log file
                let target_path = target_dir.join(&artifact.name);
                fs::copy(artifact_path, target_path).await?;
            }
        }

        info!("Extracted artifact {} to {}", artifact.name, target_dir.display());
        Ok(())
    }

    /// Get all artifacts for a task execution
    pub async fn get_task_artifacts(&self, execution_id: &Uuid) -> Vec<Artifact> {
        let artifacts = self.task_artifacts.read().await;
        artifacts.get(execution_id).cloned().unwrap_or_default()
    }

    /// Clean up old artifacts
    pub async fn cleanup_old_artifacts(&self, max_age_days: u64) -> Result<()> {
        let cutoff_time = chrono::Utc::now() - chrono::Duration::days(max_age_days as i64);
        let mut cleaned_count = 0;

        let mut artifacts = self.task_artifacts.write().await;
        let mut to_remove = Vec::new();

        for (execution_id, artifact_list) in artifacts.iter() {
            if let Some(first_artifact) = artifact_list.first() {
                if first_artifact.created_at < cutoff_time {
                    to_remove.push(*execution_id);
                    
                    // Remove artifact files
                    for artifact in artifact_list {
                        let artifact_path = Path::new(&artifact.path);
                        if artifact_path.exists() {
                            if let Err(e) = fs::remove_file(artifact_path).await {
                                warn!("Failed to remove artifact file {}: {}", 
                                      artifact_path.display(), e);
                            } else {
                                cleaned_count += 1;
                            }
                        }
                    }
                }
            }
        }

        // Remove from memory
        for execution_id in to_remove {
            artifacts.remove(&execution_id);
        }

        info!("Cleaned up {} old artifact files", cleaned_count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::TaskDefinition;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_artifact_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let artifacts_dir = temp_dir.path().join("artifacts");
        
        let manager = ArtifactManager::new(artifacts_dir.clone()).await.unwrap();
        
        assert!(artifacts_dir.exists());
    }

    #[tokio::test]
    async fn test_file_artifact_collection() {
        let temp_dir = tempdir().unwrap();
        let working_dir = temp_dir.path().join("work");
        let artifacts_dir = temp_dir.path().join("artifacts");
        
        fs::create_dir_all(&working_dir).await.unwrap();
        
        // Create a test file
        let test_file = working_dir.join("test.txt");
        fs::write(&test_file, "test content").await.unwrap();
        
        let manager = ArtifactManager::new(artifacts_dir).await.unwrap();
        
        let task = TaskDefinition::new("test_task", "Test Task");
        let execution = TaskExecution::new(Uuid::new_v4(), &task);
        let context = ExecutionContext {
            job_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            environment: HashMap::new(),
            task_outputs: HashMap::new(),
            secrets: HashMap::new(),
            working_directory: working_dir.to_string_lossy().to_string(),
        };
        
        let artifacts = manager.collect_artifacts(
            &execution,
            &context,
            &["test.txt".to_string()],
        ).await.unwrap();
        
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].name, "test.txt");
        assert_eq!(artifacts[0].artifact_type, ArtifactType::File);
    }

    #[tokio::test]
    async fn test_task_output_storage() {
        let temp_dir = tempdir().unwrap();
        let artifacts_dir = temp_dir.path().join("artifacts");
        
        let manager = ArtifactManager::new(artifacts_dir).await.unwrap();
        
        let value = serde_json::json!({"result": "success", "count": 42});
        
        manager.store_task_output("build", "result", value.clone()).await.unwrap();
        
        let retrieved = manager.get_task_output("build", "result").await.unwrap();
        assert_eq!(retrieved, Some(value));
        
        let missing = manager.get_task_output("build", "missing").await.unwrap();
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn test_execution_context_preparation() {
        let temp_dir = tempdir().unwrap();
        let artifacts_dir = temp_dir.path().join("artifacts");
        
        let manager = ArtifactManager::new(artifacts_dir).await.unwrap();
        
        // Store some outputs from dependency tasks
        manager.store_task_output("dep1", "output1", serde_json::json!("value1")).await.unwrap();
        manager.store_task_output("dep1", "output2", serde_json::json!(42)).await.unwrap();
        manager.store_task_output("dep2", "result", serde_json::json!({"status": "ok"})).await.unwrap();
        
        let base_context = ExecutionContext {
            job_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            environment: HashMap::new(),
            task_outputs: HashMap::new(),
            secrets: HashMap::new(),
            working_directory: "/tmp".to_string(),
        };
        
        let context = manager.prepare_execution_context(
            base_context,
            &["dep1".to_string(), "dep2".to_string()],
        ).await.unwrap();
        
        assert!(context.task_outputs.contains_key("dep1"));
        assert!(context.task_outputs.contains_key("dep2"));
        
        let dep1_outputs = &context.task_outputs["dep1"];
        assert_eq!(dep1_outputs["output1"], serde_json::json!("value1"));
        assert_eq!(dep1_outputs["output2"], serde_json::json!(42));
        
        let dep2_outputs = &context.task_outputs["dep2"];
        assert_eq!(dep2_outputs["result"], serde_json::json!({"status": "ok"}));
    }
}