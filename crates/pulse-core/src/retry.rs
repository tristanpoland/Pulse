use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// Comprehensive retry configuration for task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub delay_seconds: u64,
    pub strategy: RetryStrategy,
    pub exponential_backoff: Option<ExponentialBackoffConfig>,
    pub jitter: bool,
    pub retry_on: Vec<RetryCondition>,
    pub conditions: Vec<ConditionalRetryConfig>,
    pub persist_state: bool,
    pub timeout_minutes: Option<u64>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    pub stop_on_success: bool,
    pub never_give_up: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryStrategy {
    Immediate,
    Fixed,
    Exponential,
    Linear,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExponentialBackoffConfig {
    pub backoff_multiplier: f64,
    pub max_delay_seconds: u64,
    pub jitter: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryCondition {
    NetworkError,
    Timeout,
    ExitCodes(Vec<i32>),
    OutputContains(Vec<String>),
    ErrorContains(Vec<String>),
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalRetryConfig {
    pub condition: String, // Expression to evaluate
    pub max_attempts: Option<u32>,
    pub delay_seconds: Option<u64>,
    pub strategy: Option<RetryStrategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub recovery_timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRetryState {
    pub task_id: String,
    pub execution_id: Uuid,
    pub attempt_number: u32,
    pub total_attempts: u32,
    pub last_error: Option<String>,
    pub last_exit_code: Option<i32>,
    pub next_retry_at: Option<chrono::DateTime<chrono::Utc>>,
    pub persistent_state: Option<PersistentTaskState>,
    pub circuit_breaker_state: Option<CircuitBreakerState>,
    pub retry_history: Vec<RetryAttempt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentTaskState {
    pub working_directory: String,
    pub environment_variables: HashMap<String, String>,
    pub outputs: HashMap<String, serde_json::Value>,
    pub artifacts: Vec<crate::Artifact>,
    pub intermediate_results: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerState {
    pub is_open: bool,
    pub failure_count: u32,
    pub last_failure_at: chrono::DateTime<chrono::Utc>,
    pub recovery_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryAttempt {
    pub attempt_number: u32,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub result: Option<RetryAttemptResult>,
    pub delay_before_retry: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryAttemptResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub should_retry: bool,
    pub retry_reason: Option<String>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay_seconds: 30,
            strategy: RetryStrategy::Fixed,
            exponential_backoff: None,
            jitter: false,
            retry_on: vec![
                RetryCondition::NetworkError,
                RetryCondition::Timeout,
                RetryCondition::ExitCodes(vec![1]),
            ],
            conditions: Vec::new(),
            persist_state: false,
            timeout_minutes: None,
            circuit_breaker: None,
            stop_on_success: true,
            never_give_up: false,
        }
    }
}

impl RetryConfig {
    pub fn simple(max_attempts: u32, delay_seconds: u64) -> Self {
        Self {
            max_attempts,
            delay_seconds,
            ..Default::default()
        }
    }

    pub fn exponential_backoff(
        max_attempts: u32,
        initial_delay: u64,
        multiplier: f64,
        max_delay: u64,
    ) -> Self {
        Self {
            max_attempts,
            delay_seconds: initial_delay,
            strategy: RetryStrategy::Exponential,
            exponential_backoff: Some(ExponentialBackoffConfig {
                backoff_multiplier: multiplier,
                max_delay_seconds: max_delay,
                jitter: true,
            }),
            jitter: true,
            ..Default::default()
        }
    }

    pub fn with_circuit_breaker(mut self, failure_threshold: u32, recovery_timeout: u64) -> Self {
        self.circuit_breaker = Some(CircuitBreakerConfig {
            enabled: true,
            failure_threshold,
            recovery_timeout_seconds: recovery_timeout,
        });
        self
    }

    pub fn never_give_up(mut self) -> Self {
        self.never_give_up = true;
        self
    }

    pub fn persist_state(mut self) -> Self {
        self.persist_state = true;
        self
    }
}

impl TaskRetryState {
    pub fn new(task_id: String, execution_id: Uuid, config: &RetryConfig) -> Self {
        Self {
            task_id,
            execution_id,
            attempt_number: 1,
            total_attempts: config.max_attempts,
            last_error: None,
            last_exit_code: None,
            next_retry_at: None,
            persistent_state: None,
            circuit_breaker_state: None,
            retry_history: Vec::new(),
        }
    }

    pub fn can_retry(&self) -> bool {
        if self.circuit_breaker_state.as_ref().map_or(false, |cb| cb.is_open) {
            return false;
        }
        
        self.attempt_number < self.total_attempts
    }

    pub fn should_retry(&self, config: &RetryConfig, result: &crate::TaskExecutionResult) -> bool {
        if !self.can_retry() {
            return false;
        }

        if config.never_give_up && !result.success {
            return true;
        }

        // Check retry conditions
        for condition in &config.retry_on {
            if self.matches_condition(condition, result) {
                return true;
            }
        }

        false
    }

    fn matches_condition(&self, condition: &RetryCondition, result: &crate::TaskExecutionResult) -> bool {
        match condition {
            RetryCondition::NetworkError => {
                // Check for network-related errors
                result.error_message.as_ref().map_or(false, |msg| {
                    msg.to_lowercase().contains("network") ||
                    msg.to_lowercase().contains("connection") ||
                    msg.to_lowercase().contains("timeout")
                })
            }
            RetryCondition::Timeout => {
                result.error_message.as_ref().map_or(false, |msg| {
                    msg.to_lowercase().contains("timeout") ||
                    msg.to_lowercase().contains("timed out")
                })
            }
            RetryCondition::ExitCodes(codes) => {
                result.exit_code.map_or(false, |code| codes.contains(&code))
            }
            RetryCondition::OutputContains(patterns) => {
                if let Some(output) = &result.output {
                    let output_str = serde_json::to_string(output).unwrap_or_default();
                    patterns.iter().any(|pattern| output_str.contains(pattern))
                } else {
                    false
                }
            }
            RetryCondition::ErrorContains(patterns) => {
                result.error_message.as_ref().map_or(false, |error| {
                    patterns.iter().any(|pattern| error.contains(pattern))
                })
            }
            RetryCondition::Custom(_) => {
                // Custom conditions would be evaluated by expression engine
                false
            }
        }
    }

    pub fn calculate_next_delay(&self, config: &RetryConfig) -> Duration {
        let base_delay = Duration::from_secs(config.delay_seconds);
        
        match config.strategy {
            RetryStrategy::Immediate => Duration::from_secs(0),
            RetryStrategy::Fixed => base_delay,
            RetryStrategy::Linear => base_delay * (self.attempt_number as u32),
            RetryStrategy::Exponential => {
                if let Some(backoff_config) = &config.exponential_backoff {
                    let delay = base_delay.as_secs() as f64 
                        * backoff_config.backoff_multiplier.powi((self.attempt_number - 1) as i32);
                    
                    let delay = delay.min(backoff_config.max_delay_seconds as f64);
                    
                    let mut final_delay = Duration::from_secs(delay as u64);
                    
                    // Add jitter if enabled
                    if config.jitter || backoff_config.jitter {
                        final_delay = self.add_jitter(final_delay);
                    }
                    
                    final_delay
                } else {
                    // Default exponential backoff
                    let delay = base_delay.as_secs() * 2_u64.pow(self.attempt_number - 1);
                    Duration::from_secs(delay.min(300)) // Cap at 5 minutes
                }
            }
            RetryStrategy::Custom(_) => base_delay, // Would be implemented by custom logic
        }
    }

    fn add_jitter(&self, base_delay: Duration) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let jitter_factor = rng.gen_range(0.5..1.5); // ±50% jitter
        Duration::from_secs((base_delay.as_secs() as f64 * jitter_factor) as u64)
    }

    pub fn record_attempt(&mut self, result: &crate::TaskExecutionResult) {
        let attempt = RetryAttempt {
            attempt_number: self.attempt_number,
            started_at: chrono::Utc::now(),
            completed_at: Some(chrono::Utc::now()),
            result: Some(RetryAttemptResult {
                success: result.success,
                exit_code: result.exit_code,
                error_message: result.error_message.clone(),
                should_retry: !result.success,
                retry_reason: if !result.success {
                    Some("Task failed".to_string())
                } else {
                    None
                },
            }),
            delay_before_retry: Duration::from_secs(0), // Will be set when scheduling next retry
        };

        self.retry_history.push(attempt);
        self.last_error = result.error_message.clone();
        self.last_exit_code = result.exit_code;
    }

    pub fn prepare_next_retry(&mut self, config: &RetryConfig) {
        self.attempt_number += 1;
        let delay = self.calculate_next_delay(config);
        self.next_retry_at = Some(chrono::Utc::now() + chrono::Duration::from_std(delay).unwrap());
        
        // Update last attempt with delay information
        if let Some(last_attempt) = self.retry_history.last_mut() {
            last_attempt.delay_before_retry = delay;
        }
    }

    pub fn update_circuit_breaker(&mut self, config: &RetryConfig, success: bool) {
        if let Some(cb_config) = &config.circuit_breaker {
            let cb_state = self.circuit_breaker_state.get_or_insert(CircuitBreakerState {
                is_open: false,
                failure_count: 0,
                last_failure_at: chrono::Utc::now(),
                recovery_at: None,
            });

            if success {
                // Success - reset circuit breaker
                cb_state.is_open = false;
                cb_state.failure_count = 0;
                cb_state.recovery_at = None;
            } else {
                // Failure - increment counter
                cb_state.failure_count += 1;
                cb_state.last_failure_at = chrono::Utc::now();

                if cb_state.failure_count >= cb_config.failure_threshold {
                    // Open circuit breaker
                    cb_state.is_open = true;
                    cb_state.recovery_at = Some(
                        chrono::Utc::now() + chrono::Duration::seconds(cb_config.recovery_timeout_seconds as i64)
                    );
                }
            }
        }
    }

    pub fn is_circuit_breaker_ready(&self) -> bool {
        if let Some(cb_state) = &self.circuit_breaker_state {
            if cb_state.is_open {
                cb_state.recovery_at.map_or(false, |recovery| chrono::Utc::now() >= recovery)
            } else {
                true
            }
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_creation() {
        let config = RetryConfig::exponential_backoff(5, 10, 2.0, 300)
            .with_circuit_breaker(3, 120)
            .persist_state();

        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.delay_seconds, 10);
        assert!(matches!(config.strategy, RetryStrategy::Exponential));
        assert!(config.circuit_breaker.is_some());
        assert!(config.persist_state);
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = RetryConfig::exponential_backoff(5, 10, 2.0, 300);
        let mut state = TaskRetryState::new("test".to_string(), Uuid::new_v4(), &config);

        // First attempt (attempt_number = 1)
        let delay1 = state.calculate_next_delay(&config);
        assert_eq!(delay1.as_secs(), 10);

        // Second attempt
        state.attempt_number = 2;
        let delay2 = state.calculate_next_delay(&config);
        assert_eq!(delay2.as_secs(), 20);

        // Third attempt  
        state.attempt_number = 3;
        let delay3 = state.calculate_next_delay(&config);
        assert_eq!(delay3.as_secs(), 40);
    }

    #[test]
    fn test_circuit_breaker() {
        let config = RetryConfig::simple(5, 10).with_circuit_breaker(2, 60);
        let mut state = TaskRetryState::new("test".to_string(), Uuid::new_v4(), &config);

        // First failure
        state.update_circuit_breaker(&config, false);
        assert!(!state.circuit_breaker_state.as_ref().unwrap().is_open);

        // Second failure - should open circuit breaker
        state.update_circuit_breaker(&config, false);
        assert!(state.circuit_breaker_state.as_ref().unwrap().is_open);
        assert!(!state.can_retry());

        // Success should close circuit breaker
        state.update_circuit_breaker(&config, true);
        assert!(!state.circuit_breaker_state.as_ref().unwrap().is_open);
        assert!(state.can_retry());
    }
}