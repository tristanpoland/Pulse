use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::task::{TaskDefinition, TaskExecution, TaskStatus};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tasks: Vec<TaskDefinition>,
    pub executions: HashMap<String, TaskExecution>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Job {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            status: JobStatus::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            tasks: Vec::new(),
            executions: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_tasks(mut self, tasks: Vec<TaskDefinition>) -> Self {
        self.tasks = tasks;
        // Initialize executions for each task
        for task in &self.tasks {
            let execution = TaskExecution::new(self.id, task);
            self.executions.insert(task.id.clone(), execution);
        }
        self
    }

    pub fn start(&mut self) {
        self.status = JobStatus::Running;
        self.started_at = Some(Utc::now());
    }

    pub fn complete(&mut self) {
        self.status = JobStatus::Completed;
        self.completed_at = Some(Utc::now());
    }

    pub fn fail(&mut self) {
        self.status = JobStatus::Failed;
        self.completed_at = Some(Utc::now());
    }

    pub fn cancel(&mut self) {
        self.status = JobStatus::Cancelled;
        self.completed_at = Some(Utc::now());
    }

    pub fn get_ready_tasks(&self) -> Vec<&TaskDefinition> {
        self.tasks
            .iter()
            .filter(|task| {
                let execution = self.executions.get(&task.id);
                if let Some(exec) = execution {
                    if exec.status != TaskStatus::Pending {
                        return false;
                    }
                }

                // Check if all dependencies are completed
                task.needs.iter().all(|dep| {
                    if let Some(dep_exec) = self.executions.get(dep) {
                        dep_exec.status == TaskStatus::Completed
                    } else {
                        false
                    }
                })
            })
            .collect()
    }

    pub fn update_task_execution(&mut self, task_id: &str, execution: TaskExecution) {
        self.executions.insert(task_id.to_string(), execution);
        self.update_job_status();
    }

    fn update_job_status(&mut self) {
        let all_completed = self.executions.values().all(|e| e.status == TaskStatus::Completed);
        let any_failed = self.executions.values().any(|e| e.status == TaskStatus::Failed);
        let any_running = self.executions.values().any(|e| e.status == TaskStatus::Running);

        if all_completed {
            self.complete();
        } else if any_failed {
            self.fail();
        } else if any_running && self.status == JobStatus::Pending {
            self.start();
        }
    }

    pub fn get_task_output(&self, task_id: &str) -> Option<&serde_json::Value> {
        self.executions
            .get(task_id)
            .and_then(|exec| exec.output.as_ref())
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}