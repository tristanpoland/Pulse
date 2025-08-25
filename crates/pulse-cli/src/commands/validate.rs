use anyhow::{Context, Result};
use pulse_parser::WorkflowParser;
use std::path::PathBuf;

use crate::commands::formatters::{print_success, print_error, print_info};

pub async fn execute(file: PathBuf) -> Result<()> {
    print_info(&format!("Validating workflow file: {}", file.display()));

    let parser = WorkflowParser::new();
    
    match parser.parse_from_file(&file) {
        Ok(workflow) => {
            print_success("Workflow file is valid!");
            
            // Print workflow summary
            println!("Workflow Summary:");
            println!("  Name: {}", workflow.name);
            println!("  Version: {}", workflow.version);
            println!("  Jobs: {}", workflow.jobs.len());
            
            for (job_name, job) in &workflow.jobs {
                println!("    - {}: {} step(s)", job_name, job.steps.len());
                
                if let Some(needs) = &job.needs {
                    if !needs.is_empty() {
                        println!("      Depends on: {}", needs.join(", "));
                    }
                }
            }
            
            // Validate job graph
            if let Ok(execution_order) = parser.validate_job_graph(&workflow.jobs) {
                println!("  Execution order: {}", execution_order.join(" → "));
            }
        }
        Err(e) => {
            print_error(&format!("Workflow file is invalid: {}", e));
            return Err(e.into());
        }
    }

    Ok(())
}