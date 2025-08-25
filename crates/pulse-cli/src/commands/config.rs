use anyhow::{Context, Result};

use crate::config::CliConfig;
use crate::commands::formatters::{print_success, print_info, print_formatted_output};

pub async fn show(config: &CliConfig) -> Result<()> {
    print_info("Current CLI configuration:");
    
    let settings = config.list_all_settings();
    let mut output = String::new();
    
    // Format as a table
    output.push_str(&format!("{:<25} {}\n", "SETTING", "VALUE"));
    output.push_str(&"-".repeat(50));
    output.push('\n');
    
    for (key, value) in settings {
        let display_value = if key == "auth.password" && !value.is_empty() {
            "***".to_string()
        } else {
            value
        };
        
        output.push_str(&format!("{:<25} {}\n", key, display_value));
    }
    
    print_formatted_output(&output)?;
    Ok(())
}

pub async fn set(config_path: &str, key: &str, value: &str) -> Result<()> {
    print_info(&format!("Setting configuration: {} = {}", key, value));
    
    // Load existing config or create default
    let mut config = CliConfig::load(config_path).await.unwrap_or_default();
    
    // Set the value
    config.set_value(key, value)
        .with_context(|| format!("Failed to set configuration key: {}", key))?;
    
    // Validate the updated config
    config.validate()
        .context("Configuration validation failed after update")?;
    
    // Save the config
    config.save(config_path).await
        .with_context(|| format!("Failed to save configuration to: {}", config_path))?;
    
    print_success(&format!("Configuration updated: {} = {}", key, value));
    Ok(())
}

pub async fn init(config_path: &str) -> Result<()> {
    print_info(&format!("Initializing configuration file: {}", config_path));
    
    let config = CliConfig::default();
    
    config.save(config_path).await
        .with_context(|| format!("Failed to create configuration file: {}", config_path))?;
    
    print_success(&format!("Configuration file created: {}", config_path));
    
    println!("\nDefault configuration:");
    show(&config).await?;
    
    println!("\nYou can modify settings using:");
    println!("  pulse config set <key> <value>");
    println!("\nExample:");
    println!("  pulse config set cluster.endpoint http://my-cluster:8080");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_config_init() {
        let temp_file = NamedTempFile::new().unwrap();
        let config_path = temp_file.path().to_string_lossy().to_string();
        
        init(&config_path).await.unwrap();
        
        // Verify file was created and is readable
        let loaded_config = CliConfig::load(&config_path).await.unwrap();
        assert_eq!(loaded_config.cluster.endpoint, "http://localhost:8080");
    }

    #[tokio::test]
    async fn test_config_set() {
        let temp_file = NamedTempFile::new().unwrap();
        let config_path = temp_file.path().to_string_lossy().to_string();
        
        // Initialize config
        init(&config_path).await.unwrap();
        
        // Set a value
        set(&config_path, "cluster.endpoint", "http://example.com:9000").await.unwrap();
        
        // Verify the value was set
        let loaded_config = CliConfig::load(&config_path).await.unwrap();
        assert_eq!(loaded_config.cluster.endpoint, "http://example.com:9000");
    }

    #[tokio::test]
    async fn test_config_set_invalid_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let config_path = temp_file.path().to_string_lossy().to_string();
        
        // Initialize config
        init(&config_path).await.unwrap();
        
        // Try to set invalid key
        let result = set(&config_path, "invalid.key", "value").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown configuration key"));
    }

    #[tokio::test]
    async fn test_config_show() {
        let config = CliConfig::default();
        
        // This should not panic or error
        show(&config).await.unwrap();
    }

    #[tokio::test]
    async fn test_config_set_validation() {
        let temp_file = NamedTempFile::new().unwrap();
        let config_path = temp_file.path().to_string_lossy().to_string();
        
        // Initialize config
        init(&config_path).await.unwrap();
        
        // Try to set invalid timeout
        let result = set(&config_path, "cluster.timeout_seconds", "0").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("validation failed"));
    }
}