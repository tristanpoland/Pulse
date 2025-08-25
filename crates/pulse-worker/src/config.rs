use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub node: NodeConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub executor: ExecutorConfig,
    pub discovery: DiscoveryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: Uuid,
    pub name: String,
    pub labels: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub listen_address: String,
    pub bootstrap_peers: Vec<String>,
    pub heartbeat_interval_seconds: u64,
    pub connection_timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub replication_factor: usize,
    pub sync_interval_seconds: u64,
    pub max_storage_size_mb: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub max_concurrent_tasks: usize,
    pub task_timeout_seconds: Option<u64>,
    pub working_directory: String,
    pub resource_limits: ResourceLimitsConfig,
    pub enabled_executors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimitsConfig {
    pub max_memory_mb: Option<u64>,
    pub max_cpu_cores: Option<f64>,
    pub max_disk_mb: Option<u64>,
    pub max_execution_time_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    pub discovery_interval_seconds: u64,
    pub peer_timeout_seconds: u64,
    pub enable_mdns: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                id: Uuid::new_v4(),
                name: format!("pulse-worker-{}", Uuid::new_v4()),
                labels: HashMap::new(),
                metadata: HashMap::new(),
            },
            network: NetworkConfig {
                listen_address: "0.0.0.0:0".to_string(),
                bootstrap_peers: Vec::new(),
                heartbeat_interval_seconds: 30,
                connection_timeout_seconds: 10,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                replication_factor: 3,
                sync_interval_seconds: 60,
                max_storage_size_mb: Some(10240), // 10GB
            },
            executor: ExecutorConfig {
                max_concurrent_tasks: 10,
                task_timeout_seconds: Some(3600), // 1 hour
                working_directory: "./workspace".to_string(),
                resource_limits: ResourceLimitsConfig {
                    max_memory_mb: Some(2048), // 2GB
                    max_cpu_cores: Some(2.0),
                    max_disk_mb: Some(5120), // 5GB
                    max_execution_time_seconds: Some(3600), // 1 hour
                },
                enabled_executors: vec![
                    "shell".to_string(),
                    "docker".to_string(),
                    "checkout".to_string(),
                ],
            },
            discovery: DiscoveryConfig {
                discovery_interval_seconds: 30,
                peer_timeout_seconds: 120,
                enable_mdns: true,
            },
        }
    }
}

impl WorkerConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))?;
        
        Ok(config)
    }

    pub fn to_file(&self, path: &str) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .context("Failed to serialize configuration")?;
        
        fs::write(path, content)
            .with_context(|| format!("Failed to write config file: {}", path))?;
        
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        // Validate node configuration
        if self.node.name.is_empty() {
            anyhow::bail!("Node name cannot be empty");
        }

        // Validate network configuration
        if self.network.listen_address.is_empty() {
            anyhow::bail!("Listen address cannot be empty");
        }

        // Validate storage configuration
        if self.storage.data_dir.is_empty() {
            anyhow::bail!("Data directory cannot be empty");
        }

        if self.storage.replication_factor == 0 {
            anyhow::bail!("Replication factor must be greater than 0");
        }

        // Validate executor configuration
        if self.executor.max_concurrent_tasks == 0 {
            anyhow::bail!("Max concurrent tasks must be greater than 0");
        }

        if self.executor.working_directory.is_empty() {
            anyhow::bail!("Working directory cannot be empty");
        }

        if self.executor.enabled_executors.is_empty() {
            anyhow::bail!("At least one executor must be enabled");
        }

        // Validate resource limits
        if let Some(memory) = self.executor.resource_limits.max_memory_mb {
            if memory == 0 {
                anyhow::bail!("Memory limit must be greater than 0");
            }
        }

        if let Some(cpu) = self.executor.resource_limits.max_cpu_cores {
            if cpu <= 0.0 {
                anyhow::bail!("CPU limit must be greater than 0");
            }
        }

        // Validate discovery configuration
        if self.discovery.discovery_interval_seconds == 0 {
            anyhow::bail!("Discovery interval must be greater than 0");
        }

        if self.discovery.peer_timeout_seconds == 0 {
            anyhow::bail!("Peer timeout must be greater than 0");
        }

        Ok(())
    }

    pub fn create_storage_config(&self) -> pulse_core::StorageConfig {
        pulse_core::StorageConfig {
            data_dir: self.storage.data_dir.clone(),
            node_id: self.node.id,
            replication_factor: self.storage.replication_factor,
            sync_interval_seconds: self.storage.sync_interval_seconds,
            max_storage_size_mb: self.storage.max_storage_size_mb,
            custom_options: HashMap::new(),
        }
    }

    pub fn create_node_capabilities(&self) -> pulse_core::NodeCapabilities {
        let mut resource_limits = HashMap::new();
        
        if let Some(memory) = self.executor.resource_limits.max_memory_mb {
            resource_limits.insert("memory_mb".to_string(), memory);
        }
        
        if let Some(cpu) = self.executor.resource_limits.max_cpu_cores {
            resource_limits.insert("cpu_cores".to_string(), cpu as u64);
        }
        
        if let Some(disk) = self.executor.resource_limits.max_disk_mb {
            resource_limits.insert("disk_mb".to_string(), disk);
        }

        pulse_core::NodeCapabilities {
            max_concurrent_tasks: self.executor.max_concurrent_tasks,
            available_executors: self.executor.enabled_executors.clone(),
            resource_limits,
            labels: self.node.labels.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_default_config() {
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
        assert!(!config.node.name.is_empty());
        assert_eq!(config.executor.max_concurrent_tasks, 10);
        assert_eq!(config.storage.replication_factor, 3);
    }

    #[test]
    fn test_config_serialization() {
        let config = WorkerConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        assert!(toml_str.contains("[node]"));
        assert!(toml_str.contains("[network]"));
        assert!(toml_str.contains("[storage]"));
        assert!(toml_str.contains("[executor]"));
        assert!(toml_str.contains("[discovery]"));
    }

    #[test]
    fn test_config_file_operations() {
        let config = WorkerConfig::default();
        
        let mut temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        
        // Write config to file
        config.to_file(&file_path).unwrap();
        
        // Read config from file
        let loaded_config = WorkerConfig::from_file(&file_path).unwrap();
        
        // Configs should be equivalent (IDs might differ due to generation)
        assert_eq!(config.network.listen_address, loaded_config.network.listen_address);
        assert_eq!(config.executor.max_concurrent_tasks, loaded_config.executor.max_concurrent_tasks);
    }

    #[test]
    fn test_config_validation() {
        let mut config = WorkerConfig::default();
        
        // Valid config should pass
        assert!(config.validate().is_ok());
        
        // Test invalid configurations
        config.node.name = String::new();
        assert!(config.validate().is_err());
        
        config.node.name = "test".to_string();
        config.executor.max_concurrent_tasks = 0;
        assert!(config.validate().is_err());
        
        config.executor.max_concurrent_tasks = 1;
        config.storage.replication_factor = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_create_node_capabilities() {
        let config = WorkerConfig::default();
        let capabilities = config.create_node_capabilities();
        
        assert_eq!(capabilities.max_concurrent_tasks, config.executor.max_concurrent_tasks);
        assert_eq!(capabilities.available_executors, config.executor.enabled_executors);
        assert!(!capabilities.resource_limits.is_empty());
    }
}