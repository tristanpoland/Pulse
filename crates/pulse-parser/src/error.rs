use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("YAML parsing error: {0}")]
    YamlError(#[from] serde_yaml::Error),

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Missing required field: {field} in {context}")]
    MissingField { field: String, context: String },

    #[error("Invalid field value: {field} = {value} in {context}")]
    InvalidValue {
        field: String,
        value: String,
        context: String,
    },

    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Unknown action: {action}")]
    UnknownAction { action: String },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ParserError>;