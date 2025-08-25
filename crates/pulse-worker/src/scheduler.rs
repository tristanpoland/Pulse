use async_trait::async_trait;
use pulse_core::{
    AffinityRule, NodeInfo, NodeScheduler, TaskRequirements,
};
use std::collections::HashMap;
use tracing::{debug, info};
use uuid::Uuid;

pub struct PulseNodeScheduler {
    node_id: Uuid,
}

impl PulseNodeScheduler {
    pub fn new(node_id: Uuid) -> Self {
        Self { node_id }
    }

    fn calculate_node_score(
        &self,
        node: &NodeInfo,
        requirements: &TaskRequirements,
    ) -> f64 {
        let mut score = 100.0; // Base score

        // Check resource requirements
        for (resource, required_amount) in &requirements.resource_requirements {
            if let Some(available) = node.capabilities.resource_limits.get(resource) {
                let usage_ratio = *required_amount as f64 / *available as f64;
                if usage_ratio > 1.0 {
                    // Resource requirement cannot be satisfied
                    return 0.0;
                } else {
                    // Penalize high resource usage
                    score -= usage_ratio * 20.0;
                }
            } else {
                // Required resource not available
                return 0.0;
            }
        }

        // Check executor requirements
        for required_executor in &requirements.required_executors {
            if !node.capabilities.available_executors.contains(required_executor) {
                return 0.0; // Required executor not available
            }
        }

        // Check node selector requirements
        if let Some(selector) = &requirements.node_selector {
            for (key, value) in selector {
                if node.capabilities.labels.get(key) != Some(value) {
                    return 0.0; // Node selector requirement not met
                }
            }
        }

        // Apply affinity rules
        for affinity_rule in &requirements.affinity_rules {
            match affinity_rule {
                AffinityRule::PreferNode { node_id } => {
                    if node.id == *node_id {
                        score += 50.0; // Strong preference bonus
                    }
                }
                AffinityRule::AvoidNode { node_id } => {
                    if node.id == *node_id {
                        score -= 50.0; // Strong avoidance penalty
                    }
                }
                AffinityRule::RequireLabel { key, value } => {
                    if node.capabilities.labels.get(key) != Some(value) {
                        return 0.0; // Hard requirement not met
                    }
                }
                AffinityRule::PreferLabel { key, value } => {
                    if node.capabilities.labels.get(key) == Some(value) {
                        score += 25.0; // Preference bonus
                    }
                }
            }
        }

        // Consider node health and capacity
        let capacity_ratio = node.capabilities.max_concurrent_tasks as f64;
        score += capacity_ratio * 5.0; // Bonus for higher capacity

        // Prefer nodes that are not at full capacity (this would require runtime info)
        // For now, we'll use a placeholder

        score.max(0.0) // Ensure non-negative score
    }

    fn meets_requirements(&self, node: &NodeInfo, requirements: &TaskRequirements) -> bool {
        self.calculate_node_score(node, requirements) > 0.0
    }
}

#[async_trait]
impl NodeScheduler for PulseNodeScheduler {
    async fn select_node_for_task(
        &self,
        task_requirements: &TaskRequirements,
        available_nodes: &[NodeInfo],
    ) -> pulse_core::Result<Option<Uuid>> {
        if available_nodes.is_empty() {
            debug!("No available nodes for task scheduling");
            return Ok(None);
        }

        debug!("Scheduling task with requirements: {:?}", task_requirements);

        let mut best_node: Option<&NodeInfo> = None;
        let mut best_score = -1.0;

        for node in available_nodes {
            let score = self.calculate_node_score(node, task_requirements);
            
            debug!("Node {} score: {}", node.id, score);
            
            if score > best_score {
                best_score = score;
                best_node = Some(node);
            }
        }

        if let Some(selected_node) = best_node {
            info!(
                "Selected node {} for task execution (score: {})",
                selected_node.id, best_score
            );
            Ok(Some(selected_node.id))
        } else {
            debug!("No suitable node found for task requirements");
            Ok(None)
        }
    }

    async fn can_schedule_task(
        &self,
        node: &NodeInfo,
        task_requirements: &TaskRequirements,
    ) -> pulse_core::Result<bool> {
        Ok(self.meets_requirements(node, task_requirements))
    }
}

pub struct TaskSchedulingEngine {
    scheduler: PulseNodeScheduler,
}

impl TaskSchedulingEngine {
    pub fn new(node_id: Uuid) -> Self {
        Self {
            scheduler: PulseNodeScheduler::new(node_id),
        }
    }

    pub async fn schedule_task(
        &self,
        task_requirements: TaskRequirements,
        available_nodes: Vec<NodeInfo>,
    ) -> pulse_core::Result<Option<Uuid>> {
        self.scheduler
            .select_node_for_task(&task_requirements, &available_nodes)
            .await
    }

    pub async fn can_node_handle_task(
        &self,
        node: &NodeInfo,
        task_requirements: &TaskRequirements,
    ) -> pulse_core::Result<bool> {
        self.scheduler.can_schedule_task(node, task_requirements).await
    }

    pub fn create_task_requirements_from_task(
        &self,
        task: &pulse_core::TaskDefinition,
    ) -> TaskRequirements {
        let mut required_executors = Vec::new();
        let mut resource_requirements = HashMap::new();

        // Determine required executor
        if let Some(action) = &task.uses {
            if action.starts_with("docker://") || action == "docker" {
                required_executors.push("docker".to_string());
            } else if action == "actions/checkout" || action.starts_with("actions/checkout@") {
                required_executors.push("checkout".to_string());
            } else {
                required_executors.push("shell".to_string()); // Default fallback
            }
        } else if !task.command.is_empty() {
            required_executors.push("shell".to_string());
        }

        // Set basic resource requirements
        resource_requirements.insert("memory_mb".to_string(), 512); // 512MB default
        resource_requirements.insert("cpu_cores".to_string(), 1); // 1 CPU core default

        // Extract resource requirements from environment variables or task parameters
        for (key, value) in &task.environment {
            if key == "PULSE_MEMORY_MB" {
                if let Ok(memory) = value.parse::<u64>() {
                    resource_requirements.insert("memory_mb".to_string(), memory);
                }
            } else if key == "PULSE_CPU_CORES" {
                if let Ok(cpu) = value.parse::<u64>() {
                    resource_requirements.insert("cpu_cores".to_string(), cpu);
                }
            }
        }

        // Create node selector from task dependencies
        let mut node_selector = None;
        if !task.dependencies.is_empty() {
            // For tasks with dependencies, we might want specific node types
            let mut selector = HashMap::new();
            
            // Check if any dependencies require specific capabilities
            for dep in &task.dependencies {
                if dep.contains("build") {
                    selector.insert("capability".to_string(), "build".to_string());
                } else if dep.contains("test") {
                    selector.insert("capability".to_string(), "test".to_string());
                }
            }
            
            if !selector.is_empty() {
                node_selector = Some(selector);
            }
        }

        TaskRequirements {
            required_executors,
            resource_requirements,
            node_selector,
            affinity_rules: Vec::new(), // Could be populated based on task metadata
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{NodeCapabilities, NodeStatus};
    use std::collections::HashMap;

    fn create_test_node(
        id: Uuid,
        max_tasks: usize,
        executors: Vec<String>,
        labels: HashMap<String, String>,
        memory_mb: u64,
    ) -> NodeInfo {
        let mut resource_limits = HashMap::new();
        resource_limits.insert("memory_mb".to_string(), memory_mb);
        resource_limits.insert("cpu_cores".to_string(), 2);

        NodeInfo {
            id,
            address: format!("worker-{}", id),
            status: NodeStatus::Online,
            capabilities: NodeCapabilities {
                max_concurrent_tasks: max_tasks,
                available_executors: executors,
                resource_limits,
                labels,
            },
            last_seen: chrono::Utc::now(),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_basic_node_scheduling() {
        let scheduler_id = Uuid::new_v4();
        let scheduler = PulseNodeScheduler::new(scheduler_id);

        let node1 = create_test_node(
            Uuid::new_v4(),
            10,
            vec!["shell".to_string(), "docker".to_string()],
            HashMap::new(),
            2048,
        );

        let node2 = create_test_node(
            Uuid::new_v4(),
            5,
            vec!["shell".to_string()],
            HashMap::new(),
            1024,
        );

        let nodes = vec![node1.clone(), node2.clone()];

        // Test shell task scheduling
        let shell_requirements = TaskRequirements {
            required_executors: vec!["shell".to_string()],
            resource_requirements: {
                let mut reqs = HashMap::new();
                reqs.insert("memory_mb".to_string(), 512);
                reqs
            },
            node_selector: None,
            affinity_rules: Vec::new(),
        };

        let selected = scheduler
            .select_node_for_task(&shell_requirements, &nodes)
            .await
            .unwrap();

        assert!(selected.is_some());

        // Test docker task scheduling
        let docker_requirements = TaskRequirements {
            required_executors: vec!["docker".to_string()],
            resource_requirements: {
                let mut reqs = HashMap::new();
                reqs.insert("memory_mb".to_string(), 512);
                reqs
            },
            node_selector: None,
            affinity_rules: Vec::new(),
        };

        let selected = scheduler
            .select_node_for_task(&docker_requirements, &nodes)
            .await
            .unwrap();

        assert_eq!(selected, Some(node1.id)); // Only node1 has docker
    }

    #[tokio::test]
    async fn test_resource_requirements() {
        let scheduler_id = Uuid::new_v4();
        let scheduler = PulseNodeScheduler::new(scheduler_id);

        let node = create_test_node(
            Uuid::new_v4(),
            10,
            vec!["shell".to_string()],
            HashMap::new(),
            1024, // 1GB memory
        );

        let nodes = vec![node];

        // Test task that fits within limits
        let small_task = TaskRequirements {
            required_executors: vec!["shell".to_string()],
            resource_requirements: {
                let mut reqs = HashMap::new();
                reqs.insert("memory_mb".to_string(), 512); // 512MB - should fit
                reqs
            },
            node_selector: None,
            affinity_rules: Vec::new(),
        };

        let selected = scheduler
            .select_node_for_task(&small_task, &nodes)
            .await
            .unwrap();

        assert!(selected.is_some());

        // Test task that exceeds limits
        let large_task = TaskRequirements {
            required_executors: vec!["shell".to_string()],
            resource_requirements: {
                let mut reqs = HashMap::new();
                reqs.insert("memory_mb".to_string(), 2048); // 2GB - exceeds node capacity
                reqs
            },
            node_selector: None,
            affinity_rules: Vec::new(),
        };

        let selected = scheduler
            .select_node_for_task(&large_task, &nodes)
            .await
            .unwrap();

        assert!(selected.is_none()); // Should not select any node
    }

    #[tokio::test]
    async fn test_affinity_rules() {
        let scheduler_id = Uuid::new_v4();
        let scheduler = PulseNodeScheduler::new(scheduler_id);

        let preferred_node_id = Uuid::new_v4();
        let avoided_node_id = Uuid::new_v4();

        let preferred_node = create_test_node(
            preferred_node_id,
            5,
            vec!["shell".to_string()],
            HashMap::new(),
            1024,
        );

        let avoided_node = create_test_node(
            avoided_node_id,
            10,
            vec!["shell".to_string()],
            HashMap::new(),
            2048,
        );

        let nodes = vec![preferred_node, avoided_node];

        // Test preference affinity
        let requirements_with_preference = TaskRequirements {
            required_executors: vec!["shell".to_string()],
            resource_requirements: {
                let mut reqs = HashMap::new();
                reqs.insert("memory_mb".to_string(), 512);
                reqs
            },
            node_selector: None,
            affinity_rules: vec![AffinityRule::PreferNode {
                node_id: preferred_node_id,
            }],
        };

        let selected = scheduler
            .select_node_for_task(&requirements_with_preference, &nodes)
            .await
            .unwrap();

        assert_eq!(selected, Some(preferred_node_id));
    }

    #[test]
    fn test_task_requirements_from_definition() {
        let scheduler_id = Uuid::new_v4();
        let engine = TaskSchedulingEngine::new(scheduler_id);

        // Test shell task
        let shell_task = pulse_core::TaskDefinition::new("test", "Test Task")
            .with_command(vec!["echo".to_string(), "hello".to_string()]);

        let requirements = engine.create_task_requirements_from_task(&shell_task);
        assert_eq!(requirements.required_executors, vec!["shell"]);
        assert_eq!(requirements.resource_requirements.get("memory_mb"), Some(&512));

        // Test docker task
        let mut docker_task = pulse_core::TaskDefinition::new("docker-test", "Docker Task");
        docker_task.uses = Some("docker://ubuntu:latest".to_string());

        let requirements = engine.create_task_requirements_from_task(&docker_task);
        assert_eq!(requirements.required_executors, vec!["docker"]);

        // Test checkout task
        let mut checkout_task = pulse_core::TaskDefinition::new("checkout-test", "Checkout Task");
        checkout_task.uses = Some("actions/checkout@v4".to_string());

        let requirements = engine.create_task_requirements_from_task(&checkout_task);
        assert_eq!(requirements.required_executors, vec!["checkout"]);
    }
}