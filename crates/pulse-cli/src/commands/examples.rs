use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::fs;

use crate::commands::formatters::{print_success, print_info};

pub async fn execute(example_type: &str, output_file: PathBuf) -> Result<()> {
    print_info(&format!("Generating {} example workflow", example_type));

    let example_content = match example_type {
        "simple" => SIMPLE_EXAMPLE,
        "docker" => DOCKER_EXAMPLE,
        "complex" => COMPLEX_EXAMPLE,
        "matrix" => MATRIX_EXAMPLE,
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown example type: {}. Available types: simple, docker, complex, matrix",
                example_type
            ));
        }
    };

    fs::write(&output_file, example_content).await
        .with_context(|| format!("Failed to write example file: {}", output_file.display()))?;

    print_success(&format!("Example workflow written to: {}", output_file.display()));
    
    println!("\nTo submit this workflow:");
    println!("  pulse submit {}", output_file.display());
    println!("\nTo validate this workflow:");
    println!("  pulse validate {}", output_file.display());

    Ok(())
}

const SIMPLE_EXAMPLE: &str = r#"name: Simple CI Pipeline
version: "1.0"
description: A simple CI pipeline that builds and tests an application

on:
  event: push

env:
  NODE_VERSION: "18"

jobs:
  build:
    name: Build Application
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          repository: "https://github.com/example/myapp.git"
          ref: main

      - name: Setup Node.js
        run: |
          curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
          sudo apt-get install -y nodejs

      - name: Install dependencies
        run: npm ci

      - name: Build application
        run: npm run build

  test:
    name: Run Tests
    runs-on: ["ubuntu-latest"] 
    needs: ["build"]
    steps:
      - name: Run unit tests
        run: npm test

      - name: Run integration tests
        run: npm run test:integration
        env:
          NODE_ENV: test

  deploy:
    name: Deploy to Staging
    runs-on: ["ubuntu-latest"]
    needs: ["test"]
    if_condition: "success()"
    steps:
      - name: Deploy application
        run: |
          echo "Deploying to staging environment..."
          sleep 2
          echo "Deployment completed successfully"
"#;

const DOCKER_EXAMPLE: &str = r#"name: Docker Build Pipeline
version: "1.0"
description: A pipeline that builds and tests a Docker application

on:
  event: push

jobs:
  docker-build:
    name: Build Docker Image
    runs-on: ["docker-enabled"]
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Build Docker image
        uses: docker
        with:
          image: "docker:latest"
          run: |
            docker build -t myapp:latest .
            docker tag myapp:latest myapp:${{ github.sha }}

      - name: Test Docker image
        uses: docker
        with:
          image: "myapp:latest"
          run: |
            echo "Running application tests..."
            npm test

      - name: Push to registry
        uses: docker
        with:
          image: "docker:latest"
          run: |
            echo "Pushing image to registry..."
            docker push myapp:latest
            docker push myapp:${{ github.sha }}
"#;

const COMPLEX_EXAMPLE: &str = r#"name: Complex Multi-Stage Pipeline
version: "1.0"
description: A complex pipeline with multiple stages and parallel execution

on:
  event: push

env:
  APP_ENV: production
  DATABASE_URL: postgres://localhost:5432/myapp

jobs:
  lint:
    name: Code Quality Checks
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Run linter
        run: |
          npm install
          npm run lint

      - name: Run security scan
        run: npm audit --audit-level high

  unit-tests:
    name: Unit Tests
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Run unit tests
        run: |
          npm install
          npm run test:unit -- --coverage

  integration-tests:
    name: Integration Tests
    runs-on: ["ubuntu-latest"]
    needs: ["lint"]
    steps:
      - name: Start database
        uses: docker
        with:
          image: postgres:13
          ports: ["5432:5432"]
          env:
            POSTGRES_PASSWORD: testpass
            POSTGRES_DB: testdb

      - name: Run integration tests
        run: |
          npm install
          npm run test:integration

  build-frontend:
    name: Build Frontend
    runs-on: ["ubuntu-latest"] 
    needs: ["lint"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Build frontend
        run: |
          cd frontend
          npm install
          npm run build

  build-backend:
    name: Build Backend
    runs-on: ["ubuntu-latest"]
    needs: ["lint"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Build backend
        run: |
          cd backend
          npm install
          npm run build

  deploy:
    name: Deploy Application
    runs-on: ["production-server"]
    needs: ["unit-tests", "integration-tests", "build-frontend", "build-backend"]
    steps:
      - name: Deploy to production
        run: |
          echo "Deploying application..."
          ./deploy.sh
        env:
          DEPLOY_KEY: ${{ secrets.DEPLOY_KEY }}
"#;

const MATRIX_EXAMPLE: &str = r#"name: Matrix Build Pipeline
version: "1.0"
description: A pipeline that tests across multiple environments

on:
  event: push

jobs:
  test:
    name: Test on Multiple Platforms
    runs-on: ["ubuntu-latest", "windows-latest", "macos-latest"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node.js 16
        run: |
          # This would be platform-specific setup
          echo "Setting up Node.js 16"

      - name: Install dependencies
        run: npm ci

      - name: Run tests
        run: npm test

  test-node-versions:
    name: Test Node.js Versions
    runs-on: ["ubuntu-latest"]
    env:
      NODE_VERSIONS: "16,18,20"
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Test Node.js 16
        run: |
          export NODE_VERSION=16
          echo "Testing with Node.js $NODE_VERSION"
          npm ci
          npm test

      - name: Test Node.js 18
        run: |
          export NODE_VERSION=18
          echo "Testing with Node.js $NODE_VERSION" 
          npm ci
          npm test

      - name: Test Node.js 20
        run: |
          export NODE_VERSION=20
          echo "Testing with Node.js $NODE_VERSION"
          npm ci
          npm test
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_generate_simple_example() {
        let temp_dir = tempdir().unwrap();
        let output_file = temp_dir.path().join("simple.yml");
        
        execute("simple", output_file.clone()).await.unwrap();
        
        let content = fs::read_to_string(&output_file).await.unwrap();
        assert!(content.contains("Simple CI Pipeline"));
        assert!(content.contains("jobs:"));
        assert!(content.contains("build:"));
    }

    #[tokio::test] 
    async fn test_generate_docker_example() {
        let temp_dir = tempdir().unwrap();
        let output_file = temp_dir.path().join("docker.yml");
        
        execute("docker", output_file.clone()).await.unwrap();
        
        let content = fs::read_to_string(&output_file).await.unwrap();
        assert!(content.contains("Docker Build Pipeline"));
        assert!(content.contains("docker-build:"));
        assert!(content.contains("uses: docker"));
    }

    #[tokio::test]
    async fn test_invalid_example_type() {
        let temp_dir = tempdir().unwrap();
        let output_file = temp_dir.path().join("invalid.yml");
        
        let result = execute("invalid", output_file).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown example type"));
    }

    #[test]
    fn test_example_content_validity() {
        // Test that all example content is valid YAML
        use pulse_parser::WorkflowParser;
        
        let parser = WorkflowParser::new();
        
        // Test simple example
        let workflow = parser.parse_from_str(SIMPLE_EXAMPLE).unwrap();
        assert_eq!(workflow.name, "Simple CI Pipeline");
        
        // Test docker example 
        let workflow = parser.parse_from_str(DOCKER_EXAMPLE).unwrap();
        assert_eq!(workflow.name, "Docker Build Pipeline");
        
        // Test complex example
        let workflow = parser.parse_from_str(COMPLEX_EXAMPLE).unwrap(); 
        assert_eq!(workflow.name, "Complex Multi-Stage Pipeline");
        
        // Test matrix example
        let workflow = parser.parse_from_str(MATRIX_EXAMPLE).unwrap();
        assert_eq!(workflow.name, "Matrix Build Pipeline");
    }
}