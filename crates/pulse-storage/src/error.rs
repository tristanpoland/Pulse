use thiserror::Error;
use pulse_core::PulseError;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Sled database error: {0}")]
    SledError(#[from] sled::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Replication error: {0}")]
    ReplicationError(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Invalid data format: {0}")]
    InvalidDataFormat(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Core error: {0}")]
    CoreError(#[from] PulseError),
}

impl From<StorageError> for PulseError {
    fn from(err: StorageError) -> Self {
        PulseError::StorageError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;