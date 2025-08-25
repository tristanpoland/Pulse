use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Types of artifacts that can be produced by task execution
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactType {
    /// A regular file artifact
    File,
    /// A compressed archive containing multiple files/directories
    Archive,
    /// A log file containing execution logs
    Log,
}

/// Represents an artifact produced by task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    /// Unique identifier for this artifact
    pub id: Uuid,
    /// Name or relative path of the artifact
    pub name: String,
    /// Type of artifact (File, Archive, Log)
    pub artifact_type: ArtifactType,
    /// File system path where the artifact is stored
    pub path: String,
    /// Size of the artifact in bytes
    pub size_bytes: u64,
    /// When the artifact was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// ID of the task execution that produced this artifact
    pub task_execution_id: Uuid,
    /// Additional metadata for the artifact
    pub metadata: HashMap<String, String>,
}

impl Artifact {
    /// Create a new artifact
    pub fn new(
        name: impl Into<String>,
        artifact_type: ArtifactType,
        path: impl Into<String>,
        size_bytes: u64,
        task_execution_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            artifact_type,
            path: path.into(),
            size_bytes,
            created_at: chrono::Utc::now(),
            task_execution_id,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the artifact
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set multiple metadata entries
    pub fn with_metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Get the file extension if this is a file artifact
    pub fn file_extension(&self) -> Option<&str> {
        if self.artifact_type == ArtifactType::File {
            std::path::Path::new(&self.name)
                .extension()
                .and_then(|ext| ext.to_str())
        } else {
            None
        }
    }

    /// Check if this artifact matches a given pattern
    pub fn matches_pattern(&self, pattern: &str) -> bool {
        if pattern.starts_with("file:") {
            let file_pattern = pattern.strip_prefix("file:").unwrap_or(pattern);
            self.artifact_type == ArtifactType::File && self.name == file_pattern
        } else if pattern.starts_with("dir:") {
            let dir_pattern = pattern.strip_prefix("dir:").unwrap_or(pattern);
            self.artifact_type == ArtifactType::Archive && self.name.starts_with(dir_pattern)
        } else if pattern.starts_with("glob:") {
            // Basic glob pattern matching (simplified)
            let glob_pattern = pattern.strip_prefix("glob:").unwrap_or(pattern);
            self.name.contains(glob_pattern.trim_end_matches('*').trim_start_matches('*'))
        } else {
            self.name == pattern
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artifact_creation() {
        let task_id = Uuid::new_v4();
        let artifact = Artifact::new(
            "test.txt",
            ArtifactType::File,
            "/path/to/test.txt",
            1024,
            task_id,
        );

        assert_eq!(artifact.name, "test.txt");
        assert_eq!(artifact.artifact_type, ArtifactType::File);
        assert_eq!(artifact.path, "/path/to/test.txt");
        assert_eq!(artifact.size_bytes, 1024);
        assert_eq!(artifact.task_execution_id, task_id);
        assert!(artifact.metadata.is_empty());
    }

    #[test]
    fn test_artifact_with_metadata() {
        let task_id = Uuid::new_v4();
        let artifact = Artifact::new(
            "log.txt",
            ArtifactType::Log,
            "/path/to/log.txt",
            512,
            task_id,
        )
        .with_metadata("source", "stdout")
        .with_metadata("encoding", "utf-8");

        assert_eq!(artifact.metadata.get("source"), Some(&"stdout".to_string()));
        assert_eq!(artifact.metadata.get("encoding"), Some(&"utf-8".to_string()));
    }

    #[test]
    fn test_file_extension() {
        let task_id = Uuid::new_v4();
        
        let file_artifact = Artifact::new(
            "test.txt",
            ArtifactType::File,
            "/path/to/test.txt",
            1024,
            task_id,
        );
        assert_eq!(file_artifact.file_extension(), Some("txt"));

        let archive_artifact = Artifact::new(
            "archive.tar.gz",
            ArtifactType::Archive,
            "/path/to/archive.tar.gz",
            2048,
            task_id,
        );
        assert_eq!(archive_artifact.file_extension(), None);
    }

    #[test]
    fn test_pattern_matching() {
        let task_id = Uuid::new_v4();
        
        let file_artifact = Artifact::new(
            "test.txt",
            ArtifactType::File,
            "/path/to/test.txt",
            1024,
            task_id,
        );

        assert!(file_artifact.matches_pattern("file:test.txt"));
        assert!(file_artifact.matches_pattern("test.txt"));
        assert!(file_artifact.matches_pattern("glob:test*"));
        assert!(!file_artifact.matches_pattern("file:other.txt"));

        let archive_artifact = Artifact::new(
            "build/output",
            ArtifactType::Archive,
            "/path/to/build.tar.gz",
            2048,
            task_id,
        );

        assert!(archive_artifact.matches_pattern("dir:build"));
        assert!(!archive_artifact.matches_pattern("dir:src"));
    }

    #[test]
    fn test_artifact_type_serialization() {
        let file_type = ArtifactType::File;
        let archive_type = ArtifactType::Archive;
        let log_type = ArtifactType::Log;

        let file_json = serde_json::to_string(&file_type).unwrap();
        let archive_json = serde_json::to_string(&archive_type).unwrap();
        let log_json = serde_json::to_string(&log_type).unwrap();

        assert_eq!(file_json, "\"File\"");
        assert_eq!(archive_json, "\"Archive\"");
        assert_eq!(log_json, "\"Log\"");

        let file_deserialized: ArtifactType = serde_json::from_str(&file_json).unwrap();
        let archive_deserialized: ArtifactType = serde_json::from_str(&archive_json).unwrap();
        let log_deserialized: ArtifactType = serde_json::from_str(&log_json).unwrap();

        assert_eq!(file_deserialized, ArtifactType::File);
        assert_eq!(archive_deserialized, ArtifactType::Archive);
        assert_eq!(log_deserialized, ArtifactType::Log);
    }
}