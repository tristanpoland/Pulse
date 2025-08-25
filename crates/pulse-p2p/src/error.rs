use thiserror::Error;
use pulse_core::PulseError;

#[derive(Error, Debug)]
pub enum P2PError {
    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Connection failed: {0}")]
    ConnectionError(String),

    #[error("Discovery error: {0}")]
    DiscoveryError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Transport error: {0}")]
    TransportError(Box<dyn std::error::Error + Send + Sync>),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<P2PError> for PulseError {
    fn from(err: P2PError) -> Self {
        PulseError::NetworkError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, P2PError>;