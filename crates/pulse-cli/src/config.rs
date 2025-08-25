use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    pub cluster: ClusterConfig,
    pub output: OutputConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub endpoint: String,
    pub timeout_seconds: u64,
    pub retry_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub default_format: String,
    pub color: bool,
    pub timestamp_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub token: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            cluster: ClusterConfig {
                endpoint: "http://localhost:8080".to_string(),
                timeout_seconds: 30,
                retry_attempts: 3,
            },
            output: OutputConfig {
                default_format: "table".to_string(),
                color: true,
                timestamp_format: "%Y-%m-%d %H:%M:%S UTC".to_string(),
            },
            auth: AuthConfig {
                token: None,
                username: None,
                password: None,
            },
        }
    }
}

impl CliConfig {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path).await
            .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
        
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.as_ref().display()))?;
        
        Ok(config)
    }

    pub async fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .context("Failed to serialize configuration")?;
        
        fs::write(&path, content).await
            .with_context(|| format!("Failed to write config file: {}", path.as_ref().display()))?;
        
        Ok(())
    }

    pub fn set_value(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "cluster.endpoint" => self.cluster.endpoint = value.to_string(),
            "cluster.timeout_seconds" => {
                self.cluster.timeout_seconds = value.parse()
                    .context("Invalid timeout value")?;
            }
            "cluster.retry_attempts" => {
                self.cluster.retry_attempts = value.parse()
                    .context("Invalid retry attempts value")?;
            }
            "output.default_format" => {
                if !matches!(value, "table" | "json" | "yaml") {
                    anyhow::bail!("Invalid output format. Must be one of: table, json, yaml");
                }
                self.output.default_format = value.to_string();
            }
            "output.color" => {
                self.output.color = value.parse()
                    .context("Invalid color value (must be true/false)")?;
            }
            "output.timestamp_format" => {
                self.output.timestamp_format = value.to_string();
            }
            "auth.token" => {
                self.auth.token = if value.is_empty() { None } else { Some(value.to_string()) };
            }
            "auth.username" => {
                self.auth.username = if value.is_empty() { None } else { Some(value.to_string()) };
            }
            "auth.password" => {
                self.auth.password = if value.is_empty() { None } else { Some(value.to_string()) };
            }
            _ => anyhow::bail!("Unknown configuration key: {}", key),
        }
        Ok(())
    }

    pub fn get_value(&self, key: &str) -> Result<String> {
        let value = match key {
            "cluster.endpoint" => &self.cluster.endpoint,
            "cluster.timeout_seconds" => return Ok(self.cluster.timeout_seconds.to_string()),
            "cluster.retry_attempts" => return Ok(self.cluster.retry_attempts.to_string()),
            "output.default_format" => &self.output.default_format,
            "output.color" => return Ok(self.output.color.to_string()),
            "output.timestamp_format" => &self.output.timestamp_format,
            "auth.token" => return Ok(self.auth.token.as_deref().unwrap_or("").to_string()),
            "auth.username" => return Ok(self.auth.username.as_deref().unwrap_or("").to_string()),
            "auth.password" => return Ok(self.auth.password.as_deref().unwrap_or("").to_string()),
            _ => anyhow::bail!("Unknown configuration key: {}", key),
        };
        Ok(value.to_string())
    }

    pub fn list_all_settings(&self) -> Vec<(String, String)> {
        vec![
            ("cluster.endpoint".to_string(), self.cluster.endpoint.clone()),
            ("cluster.timeout_seconds".to_string(), self.cluster.timeout_seconds.to_string()),
            ("cluster.retry_attempts".to_string(), self.cluster.retry_attempts.to_string()),
            ("output.default_format".to_string(), self.output.default_format.clone()),
            ("output.color".to_string(), self.output.color.to_string()),
            ("output.timestamp_format".to_string(), self.output.timestamp_format.clone()),
            ("auth.token".to_string(), self.auth.token.as_deref().unwrap_or("").to_string()),
            ("auth.username".to_string(), self.auth.username.as_deref().unwrap_or("").to_string()),
            ("auth.password".to_string(), if self.auth.password.is_some() { "***" } else { "" }.to_string()),
        ]
    }

    pub fn validate(&self) -> Result<()> {
        // Validate cluster endpoint
        if self.cluster.endpoint.is_empty() {
            anyhow::bail!("Cluster endpoint cannot be empty");
        }

        if !self.cluster.endpoint.starts_with("http://") && !self.cluster.endpoint.starts_with("https://") {
            anyhow::bail!("Cluster endpoint must start with http:// or https://");
        }

        // Validate timeout
        if self.cluster.timeout_seconds == 0 {
            anyhow::bail!("Timeout must be greater than 0");
        }

        if self.cluster.timeout_seconds > 3600 {
            anyhow::bail!("Timeout cannot exceed 1 hour (3600 seconds)");
        }

        // Validate retry attempts
        if self.cluster.retry_attempts > 10 {
            anyhow::bail!("Retry attempts cannot exceed 10");
        }

        // Validate output format
        if !matches!(self.output.default_format.as_str(), "table" | "json" | "yaml") {
            anyhow::bail!("Invalid default output format: {}", self.output.default_format);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = CliConfig::default();
        assert_eq!(config.cluster.endpoint, "http://localhost:8080");
        assert_eq!(config.output.default_format, "table");
        assert!(config.output.color);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let mut config = CliConfig::default();
        
        // Valid config should pass
        assert!(config.validate().is_ok());
        
        // Test invalid endpoint
        config.cluster.endpoint = "invalid-url".to_string();
        assert!(config.validate().is_err());
        
        config.cluster.endpoint = "http://localhost:8080".to_string();
        
        // Test invalid timeout
        config.cluster.timeout_seconds = 0;
        assert!(config.validate().is_err());
        
        config.cluster.timeout_seconds = 4000; // Too high
        assert!(config.validate().is_err());
        
        config.cluster.timeout_seconds = 30;
        
        // Test invalid retry attempts
        config.cluster.retry_attempts = 11; // Too high
        assert!(config.validate().is_err());
        
        config.cluster.retry_attempts = 3;
        
        // Test invalid output format
        config.output.default_format = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_set_and_get_value() {
        let mut config = CliConfig::default();
        
        // Test setting cluster endpoint
        config.set_value("cluster.endpoint", "http://example.com").unwrap();
        assert_eq!(config.get_value("cluster.endpoint").unwrap(), "http://example.com");
        
        // Test setting timeout
        config.set_value("cluster.timeout_seconds", "60").unwrap();
        assert_eq!(config.get_value("cluster.timeout_seconds").unwrap(), "60");
        
        // Test setting boolean
        config.set_value("output.color", "false").unwrap();
        assert_eq!(config.get_value("output.color").unwrap(), "false");
        
        // Test invalid key
        assert!(config.set_value("invalid.key", "value").is_err());
        assert!(config.get_value("invalid.key").is_err());
    }

    #[tokio::test]
    async fn test_config_file_operations() {
        let config = CliConfig::default();
        
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();
        
        // Save config
        config.save(file_path).await.unwrap();
        
        // Load config
        let loaded_config = CliConfig::load(file_path).await.unwrap();
        
        // Should be equivalent
        assert_eq!(config.cluster.endpoint, loaded_config.cluster.endpoint);
        assert_eq!(config.output.default_format, loaded_config.output.default_format);
    }

    #[test]
    fn test_list_all_settings() {
        let config = CliConfig::default();
        let settings = config.list_all_settings();
        
        assert!(!settings.is_empty());
        assert!(settings.iter().any(|(key, _)| key == "cluster.endpoint"));
        assert!(settings.iter().any(|(key, _)| key == "output.default_format"));
    }

    #[test]
    fn test_config_serialization() {
        let config = CliConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        
        assert!(toml_str.contains("[cluster]"));
        assert!(toml_str.contains("[output]"));
        assert!(toml_str.contains("[auth]"));
        
        // Deserialize back
        let deserialized: CliConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.cluster.endpoint, deserialized.cluster.endpoint);
    }
}