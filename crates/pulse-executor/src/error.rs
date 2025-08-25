use thiserror::Error;
use pulse_core::PulseError;

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Command not found: {0}")]
    CommandNotFound(String),

    #[error("Timeout exceeded: {timeout_seconds}s")]
    TimeoutExceeded { timeout_seconds: u64 },

    #[error("Resource limit exceeded: {resource} = {limit}")]
    ResourceLimitExceeded { resource: String, limit: String },

    #[error("Action not found: {action}")]
    ActionNotFound { action: String },

    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Docker error: {0}")]
    DockerError(String),

    #[error("Core error: {0}")]
    CoreError(#[from] PulseError),
}

impl From<ExecutorError> for PulseError {
    fn from(err: ExecutorError) -> Self {
        PulseError::TaskExecutionFailed(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ExecutorError>;