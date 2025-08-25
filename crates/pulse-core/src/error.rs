use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum PulseError {
    #[error("Task execution failed: {0}")]
    TaskExecutionFailed(String),

    #[error("Job not found: {id}")]
    JobNotFound { id: Uuid },

    #[error("Task not found: {id}")]
    TaskNotFound { id: Uuid },

    #[error("Workflow parsing error: {0}")]
    WorkflowParsingError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Cluster error: {0}")]
    ClusterError(String),

    #[error("Dependency cycle detected in workflow")]
    DependencyCycle,

    #[error("Missing dependency: {dependency} required by task {task}")]
    MissingDependency { task: Uuid, dependency: String },

    #[error("Invalid workflow: {0}")]
    InvalidWorkflow(String),

    #[error("Node not found: {id}")]
    NodeNotFound { id: Uuid },

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, PulseError>;