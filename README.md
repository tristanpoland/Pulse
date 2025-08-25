<p align="center">
  <img src="branding/logo.svg" alt="Pulse Logo" width=500"/>
</p>

Pulse is a distributed workflow engine built in Rust that executes tasks across a P2P cluster with built-in replicated storage. It features a YAML-based workflow definition format similar to GitHub Actions.

## Features

- **Distributed Execution**: Tasks run across a P2P cluster of worker nodes
- **Built-in Replicated Storage**: Automatic data replication across nodes
- **YAML Workflow Definition**: GitHub Actions-like syntax for defining workflows
- **Task Dependencies**: Complex dependency graphs with automatic resolution
- **Multiple Executors**: Shell commands, Docker containers, and custom actions
- **P2P Discovery**: Automatic node discovery and cluster coordination
- **CLI Management**: Command-line interface for job submission and monitoring
- **Production Ready**: Comprehensive error handling and recovery mechanisms

## Architecture

### Core Components

- **CLI (`pulse-cli`)**: Command-line interface for job submission and cluster management
- **Worker Node (`pulse-worker`)**: P2P worker that executes tasks
- **Core Types (`pulse-core`)**: Shared data structures and interfaces
- **Storage (`pulse-storage`)**: Replicated storage using Sled database
- **P2P Networking (`pulse-p2p`)**: libp2p-based cluster coordination
- **Executor (`pulse-executor`)**: Task execution engines (shell, docker, etc.)
- **Parser (`pulse-parser`)**: YAML workflow definition parser

### Architecture Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Pulse CLI     │    │  Worker Node 1  │    │  Worker Node 2  │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Parser    │ │    │ │  Executor   │ │    │ │  Executor   │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Client    │ │◄──►│ │ P2P Network │ │◄──►│ │ P2P Network │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    │ ┌─────────────┐ │    │ ┌─────────────┐ │
                       │ │   Storage   │ │◄──►│ │   Storage   │ │
                       │ └─────────────┘ │    │ └─────────────┘ │
                       └─────────────────┘    └─────────────────┘
```

## Quick Start

### 1. Build the Project

```bash
cargo build --release
```

### 2. Start Worker Nodes

Start the first worker node:

```bash
./target/release/pulse-worker --listen-addr 127.0.0.1:8080 --data-dir ./data/worker1
```

Start additional worker nodes:

```bash
./target/release/pulse-worker --listen-addr 127.0.0.1:8081 --data-dir ./data/worker2 --bootstrap-peers 127.0.0.1:8080
./target/release/pulse-worker --listen-addr 127.0.0.1:8082 --data-dir ./data/worker3 --bootstrap-peers 127.0.0.1:8080
```

### 3. Submit a Workflow

Create a workflow file `example.yml`:

```yaml
name: Simple CI Pipeline
version: "1.0"

on:
  event: push

jobs:
  build:
    runs-on: ["ubuntu-latest"]
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          repository: "https://github.com/example/myapp.git"
          
      - name: Build application
        run: |
          npm install
          npm run build

  test:
    runs-on: ["ubuntu-latest"]
    needs: ["build"]
    steps:
      - name: Run tests
        run: npm test
```

Submit the workflow:

```bash
./target/release/pulse submit example.yml --wait
```

### 4. Monitor Jobs and Cluster

List all jobs:

```bash
./target/release/pulse list
```

Get job details:

```bash
./target/release/pulse get <job-id> --tasks
```

View cluster status:

```bash
./target/release/pulse cluster
```

Follow job logs:

```bash
./target/release/pulse logs <job-id> --follow
```

## Workflow Syntax

Pulse uses a YAML-based workflow definition format inspired by GitHub Actions:

### Basic Structure

```yaml
name: Workflow Name
version: "1.0"
description: Optional description

on:
  event: push  # or schedule: { cron: "0 0 * * *" }

env:
  GLOBAL_VAR: value

jobs:
  job-name:
    name: Human-readable name
    runs-on: ["label1", "label2"]  # Node selectors
    needs: ["other-job"]           # Dependencies
    env:
      JOB_VAR: value
    steps:
      - name: Step name
        uses: action-name           # Use an action
        with:
          parameter: value
      
      - name: Shell command
        run: |
          echo "Running shell commands"
          ./build.sh
        env:
          STEP_VAR: value
```

### Available Actions

#### Built-in Actions

- `actions/checkout@v4`: Clone a Git repository
- `shell`: Execute shell commands (default)
- `docker`: Run Docker containers

#### Custom Actions

```yaml
steps:
  - name: Custom Docker action
    uses: docker://ubuntu:latest
    with:
      run: |
        echo "Running in Ubuntu container"
        apt-get update && apt-get install -y curl
```

### Advanced Features

#### Matrix Builds

```yaml
jobs:
  test:
    runs-on: ["ubuntu-latest"]
    env:
      NODE_VERSIONS: "16,18,20"
    steps:
      - name: Test Node 16
        run: |
          export NODE_VERSION=16
          npm test
      - name: Test Node 18  
        run: |
          export NODE_VERSION=18
          npm test
```

#### Conditional Execution

```yaml
jobs:
  deploy:
    runs-on: ["production"]
    if_condition: "success()"
    steps:
      - name: Deploy only on success
        run: ./deploy.sh
```

#### Resource Requirements

```yaml
jobs:
  heavy-job:
    runs-on: ["high-memory"]
    steps:
      - name: Memory intensive task
        run: ./process-large-data.sh
        env:
          PULSE_MEMORY_MB: "8192"  # Request 8GB RAM
          PULSE_CPU_CORES: "4"     # Request 4 CPU cores
```

## Configuration

### CLI Configuration

Initialize CLI configuration:

```bash
pulse config init
```

Set cluster endpoint:

```bash
pulse config set cluster.endpoint http://my-cluster:8080
```

View current configuration:

```bash
pulse config show
```

### Worker Configuration

Workers can be configured via TOML files or command-line arguments:

```toml
[node]
name = "worker-1"
labels = { capability = "build", region = "us-west" }

[network]
listen_address = "0.0.0.0:8080"
bootstrap_peers = ["10.0.0.1:8080", "10.0.0.2:8080"]

[executor]
max_concurrent_tasks = 10
enabled_executors = ["shell", "docker", "checkout"]

[storage]
data_dir = "/var/lib/pulse"
replication_factor = 3
```

## Commands Reference

### CLI Commands

```bash
# Submit a workflow
pulse submit workflow.yml [--name job-name] [--wait] [--output json|yaml|table]

# List jobs
pulse list [--status pending|running|completed|failed] [--recent N] [--output json|yaml|table]

# Get job details
pulse get <job-id> [--tasks] [--output json|yaml|table]

# Cancel a job
pulse cancel <job-id>

# View cluster status
pulse cluster [--output json|yaml|table]

# View logs
pulse logs <job-id> [--task task-id] [--follow] [--lines N]

# Validate workflow
pulse validate workflow.yml

# Generate examples
pulse examples [simple|docker|complex|matrix] [--output file.yml]

# Configuration management
pulse config show
pulse config set <key> <value>
pulse config init
```

### Worker Commands

```bash
# Start worker
pulse-worker [--config worker.toml] [--listen-addr addr] [--bootstrap-peers peer1,peer2] [--data-dir path]

# Worker options
--node-id <uuid>           # Specific node ID
--max-tasks <N>            # Maximum concurrent tasks
--verbose                  # Enable debug logging
```

## Development

### Building from Source

Requirements:
- Rust 1.70+
- tokio runtime
- libp2p dependencies

```bash
git clone https://github.com/pulse-org/pulse.git
cd pulse
cargo build --release
```

### Running Tests

```bash
cargo test --all
```

### Project Structure

```
pulse/
├── crates/
│   ├── pulse-cli/          # Command-line interface
│   ├── pulse-worker/       # Worker node implementation
│   ├── pulse-core/         # Core types and traits
│   ├── pulse-storage/      # Replicated storage
│   ├── pulse-p2p/         # P2P networking
│   ├── pulse-executor/     # Task executors
│   └── pulse-parser/       # Workflow parser
├── examples/              # Example workflows
├── docs/                  # Documentation
└── README.md
```

## Examples

See the `examples/` directory for workflow examples:

- `examples/simple-ci.yml` - Basic CI pipeline
- `examples/docker-build.yml` - Docker-based builds
- `examples/complex-pipeline.yml` - Multi-stage pipeline
- `examples/matrix-build.yml` - Matrix builds

Generate examples:

```bash
pulse examples simple --output my-workflow.yml
pulse examples docker --output docker-workflow.yml
pulse examples complex --output complex-workflow.yml
```

## Production Deployment

### Docker Deployment

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/pulse-worker /usr/local/bin/
COPY --from=builder /app/target/release/pulse /usr/local/bin/
EXPOSE 8080
CMD ["pulse-worker"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pulse-worker
spec:
  serviceName: pulse-worker
  replicas: 3
  template:
    spec:
      containers:
      - name: pulse-worker
        image: pulse:latest
        ports:
        - containerPort: 8080
        env:
        - name: PULSE_LISTEN_ADDR
          value: "0.0.0.0:8080"
        - name: PULSE_DATA_DIR
          value: "/data"
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

### Monitoring

Pulse exposes Prometheus metrics on `/metrics`:

- `pulse_jobs_total` - Total jobs processed
- `pulse_tasks_running` - Currently running tasks
- `pulse_cluster_nodes` - Number of cluster nodes
- `pulse_storage_size_bytes` - Storage usage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

- GitHub Issues: Report bugs and request features
- Documentation: Full documentation at docs/
- Examples: See examples/ directory

---

**Pulse** - Distributed workflow execution made simple.