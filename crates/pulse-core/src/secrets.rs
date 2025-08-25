use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Secret storage and management for workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretManager {
    secrets: HashMap<String, SecretValue>,
    namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretValue {
    pub name: String,
    pub value: String,  // Encrypted in production
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub created_by: Option<Uuid>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretReference {
    pub name: String,
    pub namespace: Option<String>,
}

impl SecretManager {
    pub fn new(namespace: String) -> Self {
        Self {
            secrets: HashMap::new(),
            namespace,
        }
    }

    /// Add a secret to the manager
    pub fn add_secret(&mut self, name: String, value: String, description: Option<String>) {
        let secret = SecretValue {
            name: name.clone(),
            value, // In production, this would be encrypted
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            created_by: None,
            description,
        };
        
        self.secrets.insert(name, secret);
    }

    /// Get a secret value by name
    pub fn get_secret(&self, name: &str) -> Option<&str> {
        self.secrets.get(name).map(|s| s.value.as_str())
    }

    /// List all secret names (not values)
    pub fn list_secret_names(&self) -> Vec<String> {
        self.secrets.keys().cloned().collect()
    }

    /// Remove a secret
    pub fn remove_secret(&mut self, name: &str) -> bool {
        self.secrets.remove(name).is_some()
    }

    /// Get namespace
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// Resolve secret references in strings
pub fn resolve_secrets(input: &str, secrets: &SecretManager) -> String {
    let mut result = input.to_string();
    
    // Find all secret references in the format ${{ secrets.SECRET_NAME }}
    while let Some(start) = result.find("${{ secrets.") {
        if let Some(end) = result[start..].find(" }}") {
            let full_ref = &result[start..start + end + 3];
            let secret_name = &result[start + 12..start + end]; // Skip "${{ secrets."
            
            if let Some(secret_value) = secrets.get_secret(secret_name) {
                result = result.replace(full_ref, secret_value);
            } else {
                // Leave unresolved references as-is or replace with empty string
                tracing::warn!("Secret not found: {}", secret_name);
                result = result.replace(full_ref, "");
            }
        } else {
            break;
        }
    }
    
    result
}

/// Resolve secrets in environment variables
pub fn resolve_env_secrets(env: HashMap<String, String>, secrets: &SecretManager) -> HashMap<String, String> {
    env.into_iter()
        .map(|(key, value)| (key, resolve_secrets(&value, secrets)))
        .collect()
}

/// Resolve secrets in command arguments
pub fn resolve_command_secrets(command: Vec<String>, secrets: &SecretManager) -> Vec<String> {
    command.into_iter()
        .map(|arg| resolve_secrets(&arg, secrets))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_manager() {
        let mut secrets = SecretManager::new("test".to_string());
        
        secrets.add_secret("API_KEY".to_string(), "secret123".to_string(), Some("Test API key".to_string()));
        secrets.add_secret("PASSWORD".to_string(), "password456".to_string(), None);
        
        assert_eq!(secrets.get_secret("API_KEY"), Some("secret123"));
        assert_eq!(secrets.get_secret("PASSWORD"), Some("password456"));
        assert_eq!(secrets.get_secret("NOT_FOUND"), None);
        
        let names = secrets.list_secret_names();
        assert!(names.contains(&"API_KEY".to_string()));
        assert!(names.contains(&"PASSWORD".to_string()));
    }

    #[test]
    fn test_resolve_secrets() {
        let mut secrets = SecretManager::new("test".to_string());
        secrets.add_secret("API_KEY".to_string(), "secret123".to_string(), None);
        secrets.add_secret("TOKEN".to_string(), "token456".to_string(), None);
        
        let input = "curl -H 'Authorization: Bearer ${{ secrets.API_KEY }}' -H 'Token: ${{ secrets.TOKEN }}' https://api.example.com";
        let result = resolve_secrets(input, &secrets);
        
        assert_eq!(result, "curl -H 'Authorization: Bearer secret123' -H 'Token: token456' https://api.example.com");
    }

    #[test]
    fn test_resolve_env_secrets() {
        let mut secrets = SecretManager::new("test".to_string());
        secrets.add_secret("DB_PASSWORD".to_string(), "dbpass123".to_string(), None);
        
        let mut env = HashMap::new();
        env.insert("DATABASE_URL".to_string(), "postgres://user:${{ secrets.DB_PASSWORD }}@localhost/db".to_string());
        env.insert("NODE_ENV".to_string(), "production".to_string());
        
        let resolved = resolve_env_secrets(env, &secrets);
        
        assert_eq!(resolved.get("DATABASE_URL"), Some(&"postgres://user:dbpass123@localhost/db".to_string()));
        assert_eq!(resolved.get("NODE_ENV"), Some(&"production".to_string()));
    }

    #[test]
    fn test_resolve_command_secrets() {
        let mut secrets = SecretManager::new("test".to_string());
        secrets.add_secret("PASSWORD".to_string(), "secret123".to_string(), None);
        
        let command = vec![
            "mysql".to_string(),
            "-u".to_string(),
            "root".to_string(),
            "-p${{ secrets.PASSWORD }}".to_string(),
            "-e".to_string(),
            "SELECT 1".to_string(),
        ];
        
        let resolved = resolve_command_secrets(command, &secrets);
        
        assert_eq!(resolved[3], "-psecret123");
    }

    #[test]
    fn test_unresolved_secrets() {
        let secrets = SecretManager::new("test".to_string());
        let input = "Token: ${{ secrets.MISSING_SECRET }}";
        let result = resolve_secrets(input, &secrets);
        
        // Unresolved secrets become empty strings
        assert_eq!(result, "Token: ");
    }
}