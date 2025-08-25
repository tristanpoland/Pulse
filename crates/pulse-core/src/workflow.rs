use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::task::TaskDefinition;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub on: WorkflowTrigger,
    pub env: Option<HashMap<String, String>>,
    pub jobs: HashMap<String, JobDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    pub name: Option<String>,
    pub runs_on: Vec<String>, // Node selectors/labels
    pub needs: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    pub steps: Vec<StepDefinition>,
    pub if_condition: Option<String>, // Conditional execution
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDefinition {
    pub id: Option<String>,
    pub name: Option<String>,
    pub uses: Option<String>, // Action to use
    pub run: Option<String>,  // Shell command to run
    pub with: Option<HashMap<String, serde_json::Value>>, // Action parameters
    pub env: Option<HashMap<String, String>>,
    pub if_condition: Option<String>,
    pub timeout_minutes: Option<u64>,
    pub continue_on_error: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkflowTrigger {
    Manual,
    Schedule { cron: String },
    Event { event: String },
    Multiple(Vec<WorkflowTrigger>),
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), String> {
        // Check for circular dependencies
        for (job_name, job) in &self.jobs {
            if let Some(needs) = &job.needs {
                if self.has_circular_dependency(job_name, needs, &HashMap::new())? {
                    return Err(format!("Circular dependency detected involving job: {}", job_name));
                }
            }
        }

        // Validate that all needed jobs exist
        for (job_name, job) in &self.jobs {
            if let Some(needs) = &job.needs {
                for needed_job in needs {
                    if !self.jobs.contains_key(needed_job) {
                        return Err(format!(
                            "Job '{}' depends on non-existent job '{}'",
                            job_name, needed_job
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn has_circular_dependency(
        &self,
        job_name: &str,
        needs: &[String],
        visited: &HashMap<String, bool>,
    ) -> Result<bool, String> {
        let mut new_visited = visited.clone();
        
        if new_visited.contains_key(job_name) {
            return Ok(true); // Circular dependency found
        }
        
        new_visited.insert(job_name.to_string(), true);

        for needed_job in needs {
            if let Some(job) = self.jobs.get(needed_job) {
                if let Some(nested_needs) = &job.needs {
                    if self.has_circular_dependency(needed_job, nested_needs, &new_visited)? {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    pub fn to_tasks(&self) -> Result<Vec<TaskDefinition>, String> {
        let mut tasks = Vec::new();
        
        for (job_name, job) in &self.jobs {
            for (step_index, step) in job.steps.iter().enumerate() {
                let task_id = step.id.clone().unwrap_or_else(|| {
                    format!("{}_{}", job_name, step_index)
                });

                let mut task = TaskDefinition::new(
                    &task_id,
                    step.name.clone().unwrap_or_else(|| task_id.clone())
                );

                // Set command based on step definition
                if let Some(run_cmd) = &step.run {
                    task.command = vec!["sh".to_string(), "-c".to_string(), run_cmd.clone()];
                } else if let Some(action) = &step.uses {
                    task.uses = Some(action.clone());
                    task.with = step.with.clone();
                }

                // Set dependencies
                if let Some(job_needs) = &job.needs {
                    task.needs = job_needs.clone();
                }

                // Set environment variables
                let mut env = HashMap::new();
                if let Some(global_env) = &self.env {
                    env.extend(global_env.clone());
                }
                if let Some(job_env) = &job.env {
                    env.extend(job_env.clone());
                }
                if let Some(step_env) = &step.env {
                    env.extend(step_env.clone());
                }
                task.environment = env;

                // Set timeout
                if let Some(timeout_min) = step.timeout_minutes {
                    task.timeout_seconds = Some(timeout_min * 60);
                }

                tasks.push(task);
            }
        }

        Ok(tasks)
    }
}