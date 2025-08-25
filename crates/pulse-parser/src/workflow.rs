use pulse_core::{Job, TaskDefinition, WorkflowDefinition};
use serde_yaml;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use uuid::Uuid;

use crate::error::{ParserError, Result};

pub struct WorkflowParser;

impl WorkflowParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_from_file<P: AsRef<Path>>(&self, path: P) -> Result<WorkflowDefinition> {
        let content = fs::read_to_string(path)?;
        self.parse_from_str(&content)
    }

    pub fn parse_from_str(&self, content: &str) -> Result<WorkflowDefinition> {
        let workflow: WorkflowDefinition = serde_yaml::from_str(content)?;
        self.validate_workflow(&workflow)?;
        Ok(workflow)
    }

    pub fn workflow_to_job(&self, workflow: &WorkflowDefinition) -> Result<Job> {
        let tasks = workflow.to_tasks()
            .map_err(|e| ParserError::ValidationError(e))?;

        let job = Job::new(&workflow.name).with_tasks(tasks);
        Ok(job)
    }

    fn validate_workflow(&self, workflow: &WorkflowDefinition) -> Result<()> {
        workflow.validate()
            .map_err(|e| ParserError::ValidationError(e))?;

        // Additional validations specific to our parser
        self.validate_job_references(workflow)?;
        self.validate_step_definitions(workflow)?;
        self.validate_environment_variables(workflow)?;

        Ok(())
    }

    fn validate_job_references(&self, workflow: &WorkflowDefinition) -> Result<()> {
        for (job_name, job) in &workflow.jobs {
            // Validate needs references
            if let Some(needs) = &job.needs {
                for needed_job in needs {
                    if !workflow.jobs.contains_key(needed_job) {
                        return Err(ParserError::ValidationError(format!(
                            "Job '{}' references non-existent job '{}' in needs",
                            job_name, needed_job
                        )));
                    }
                }
            }

            // Validate runs-on is not empty
            if job.runs_on.is_empty() {
                return Err(ParserError::ValidationError(format!(
                    "Job '{}' must specify at least one target in 'runs-on'",
                    job_name
                )));
            }
        }
        Ok(())
    }

    fn validate_step_definitions(&self, workflow: &WorkflowDefinition) -> Result<()> {
        for (job_name, job) in &workflow.jobs {
            for (step_index, step) in job.steps.iter().enumerate() {
                let step_context = format!("job '{}' step {}", job_name, step_index);

                // Each step must have either 'run' or 'uses'
                if step.run.is_none() && step.uses.is_none() {
                    return Err(ParserError::ValidationError(format!(
                        "Step in {} must specify either 'run' or 'uses'",
                        step_context
                    )));
                }

                // Can't have both 'run' and 'uses'
                if step.run.is_some() && step.uses.is_some() {
                    return Err(ParserError::ValidationError(format!(
                        "Step in {} cannot specify both 'run' and 'uses'",
                        step_context
                    )));
                }

                // Validate timeout
                if let Some(timeout) = step.timeout_minutes {
                    if timeout == 0 || timeout > 10080 { // Max 1 week
                        return Err(ParserError::ValidationError(format!(
                            "Step timeout in {} must be between 1 and 10080 minutes",
                            step_context
                        )));
                    }
                }

                // Validate action format if using 'uses'
                if let Some(action) = &step.uses {
                    self.validate_action_format(action, &step_context)?;
                }
            }
        }
        Ok(())
    }

    fn validate_action_format(&self, action: &str, context: &str) -> Result<()> {
        // Basic action format validation (e.g., "actions/checkout@v4")
        if action.is_empty() {
            return Err(ParserError::ValidationError(format!(
                "Action in {} cannot be empty",
                context
            )));
        }

        // Check for valid action format patterns
        let valid_patterns = [
            r"^[\w\-\.]+/[\w\-\.]+@[\w\-\.]+$", // org/action@version
            r"^\./.+$", // ./local-action
            r"^docker://.*$", // docker://image
        ];

        let is_valid = valid_patterns.iter().any(|pattern| {
            regex::Regex::new(pattern)
                .map(|re| re.is_match(action))
                .unwrap_or(false)
        });

        if !is_valid {
            return Err(ParserError::ValidationError(format!(
                "Invalid action format '{}' in {}. Expected formats: 'org/action@version', './local-action', or 'docker://image'",
                action, context
            )));
        }

        Ok(())
    }

    fn validate_environment_variables(&self, workflow: &WorkflowDefinition) -> Result<()> {
        // Validate global environment variables
        if let Some(env) = &workflow.env {
            for (key, value) in env {
                self.validate_env_var(key, value, "workflow level")?;
            }
        }

        // Validate job-level environment variables
        for (job_name, job) in &workflow.jobs {
            if let Some(env) = &job.env {
                for (key, value) in env {
                    self.validate_env_var(key, value, &format!("job '{}'", job_name))?;
                }
            }

            // Validate step-level environment variables
            for (step_index, step) in job.steps.iter().enumerate() {
                if let Some(env) = &step.env {
                    for (key, value) in env {
                        self.validate_env_var(
                            key,
                            value,
                            &format!("job '{}' step {}", job_name, step_index),
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_env_var(&self, key: &str, value: &str, context: &str) -> Result<()> {
        // Environment variable key validation
        if key.is_empty() {
            return Err(ParserError::ValidationError(format!(
                "Environment variable key cannot be empty in {}",
                context
            )));
        }

        // Check for valid environment variable name format
        if !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
            || key.chars().next().map_or(false, |c| c.is_ascii_digit())
        {
            return Err(ParserError::ValidationError(format!(
                "Invalid environment variable name '{}' in {}. Must contain only alphanumeric characters and underscores, and not start with a digit",
                key, context
            )));
        }

        // Check for potentially sensitive values (warn but don't fail)
        if value.len() > 1000 {
            return Err(ParserError::ValidationError(format!(
                "Environment variable '{}' value is too long ({} characters) in {}",
                key,
                value.len(),
                context
            )));
        }

        Ok(())
    }

    pub fn validate_job_graph(&self, jobs: &HashMap<String, pulse_core::JobDefinition>) -> Result<Vec<String>> {
        let mut execution_order = Vec::new();
        let mut visited = HashMap::new();
        let mut visiting = HashMap::new();

        for job_name in jobs.keys() {
            if !visited.contains_key(job_name) {
                self.topological_sort(job_name, jobs, &mut visited, &mut visiting, &mut execution_order)?;
            }
        }

        execution_order.reverse(); // Reverse to get correct dependency order
        Ok(execution_order)
    }

    fn topological_sort(
        &self,
        job_name: &str,
        jobs: &HashMap<String, pulse_core::JobDefinition>,
        visited: &mut HashMap<String, bool>,
        visiting: &mut HashMap<String, bool>,
        result: &mut Vec<String>,
    ) -> Result<()> {
        if visiting.contains_key(job_name) {
            return Err(ParserError::CircularDependency(format!(
                "Circular dependency detected involving job: {}",
                job_name
            )));
        }

        if visited.contains_key(job_name) {
            return Ok(());
        }

        visiting.insert(job_name.to_string(), true);

        if let Some(job) = jobs.get(job_name) {
            if let Some(needs) = &job.needs {
                for dependency in needs {
                    self.topological_sort(dependency, jobs, visited, visiting, result)?;
                }
            }
        }

        visiting.remove(job_name);
        visited.insert(job_name.to_string(), true);
        result.push(job_name.to_string());

        Ok(())
    }
}

impl Default for WorkflowParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_workflow() {
        let yaml = r#"
name: Simple CI
version: "1.0"
on:
  event: push
jobs:
  test:
    name: Run Tests
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4
      - name: Run tests
        run: npm test
"#;

        let parser = WorkflowParser::new();
        let result = parser.parse_from_str(yaml);
        assert!(result.is_ok());

        let workflow = result.unwrap();
        assert_eq!(workflow.name, "Simple CI");
        assert_eq!(workflow.jobs.len(), 1);
        assert!(workflow.jobs.contains_key("test"));
    }

    #[test]
    fn test_parse_workflow_with_dependencies() {
        let yaml = r#"
name: Multi-job CI
version: "1.0"
on:
  event: push
jobs:
  build:
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Build
        run: npm run build
  test:
    runs-on: ["ubuntu-latest"]
    needs: ["build"]
    steps:
      - name: Test
        run: npm test
  deploy:
    runs-on: ["ubuntu-latest"]
    needs: ["test"]
    steps:
      - name: Deploy
        run: npm run deploy
"#;

        let parser = WorkflowParser::new();
        let result = parser.parse_from_str(yaml);
        assert!(result.is_ok());

        let workflow = result.unwrap();
        let execution_order = parser.validate_job_graph(&workflow.jobs).unwrap();
        assert_eq!(execution_order, vec!["build", "test", "deploy"]);
    }
}