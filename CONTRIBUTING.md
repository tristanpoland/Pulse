# Contributing to Pulse

Thank you for your interest in contributing to Pulse! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Git
- Docker (optional, for testing Docker executors)

### Setting up the Development Environment

1. Clone the repository:
```bash
git clone https://github.com/pulse-org/pulse.git
cd pulse
```

2. Build the project:
```bash
cargo build
```

3. Run tests:
```bash
cargo test --all
```

4. Run with development logging:
```bash
RUST_LOG=debug cargo run --bin pulse-worker
```

## Project Structure

```
pulse/
├── crates/
│   ├── pulse-cli/          # Command-line interface
│   ├── pulse-worker/       # Worker node implementation  
│   ├── pulse-core/         # Core types and traits
│   ├── pulse-storage/      # Replicated storage layer
│   ├── pulse-p2p/         # P2P networking and discovery
│   ├── pulse-executor/     # Task execution engines
│   └── pulse-parser/       # YAML workflow parser
├── examples/              # Example workflows
├── docs/                  # Documentation
├── tests/                 # Integration tests
└── benches/              # Benchmarks
```

## Contributing Guidelines

### Code Style

- Follow standard Rust formatting with `rustfmt`
- Run `cargo clippy` to catch common mistakes
- Use meaningful variable and function names
- Add documentation for public APIs
- Include unit tests for new functionality

Format your code before submitting:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features
```

### Commit Messages

Use conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `chore`: Maintenance tasks

Examples:
```
feat(executor): add Docker container executor
fix(p2p): resolve connection timeout in cluster discovery
docs(readme): update installation instructions
```

### Pull Request Process

1. Create a feature branch from `main`:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and add tests
3. Ensure all tests pass:
```bash
cargo test --all
```

4. Update documentation if needed
5. Push your branch and create a pull request
6. Address any feedback from code review

### Testing

#### Unit Tests

Run unit tests for a specific crate:
```bash
cargo test -p pulse-core
```

Run all unit tests:
```bash
cargo test --lib --all
```

#### Integration Tests

Integration tests are in the `tests/` directory:
```bash
cargo test --test integration
```

#### Adding Tests

- Add unit tests in the same file as the code being tested
- Use the `#[cfg(test)]` attribute for test modules
- Test both success and error cases
- Use descriptive test names

Example:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_creation() {
        let job = Job::new("test-job");
        assert_eq!(job.name, "test-job");
        assert_eq!(job.status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_async_functionality() {
        let result = async_function().await;
        assert!(result.is_ok());
    }
}
```

### Documentation

- Add doc comments for all public functions and types
- Include examples in doc comments when helpful
- Update the README.md for significant changes
- Add entries to CHANGELOG.md for releases

Doc comment example:
```rust
/// Creates a new job with the given name.
///
/// # Arguments
///
/// * `name` - The name of the job
///
/// # Returns
///
/// A new `Job` instance in pending status
///
/// # Example
///
/// ```rust
/// let job = Job::new("my-job");
/// assert_eq!(job.name, "my-job");
/// ```
pub fn new(name: impl Into<String>) -> Self {
    // implementation
}
```

## Development Workflow

### Local Testing

1. Start a local worker cluster:
```bash
# Terminal 1
cargo run --bin pulse-worker -- --listen-addr 127.0.0.1:8080 --data-dir ./data/w1

# Terminal 2  
cargo run --bin pulse-worker -- --listen-addr 127.0.0.1:8081 --data-dir ./data/w2 --bootstrap-peers 127.0.0.1:8080

# Terminal 3
cargo run --bin pulse-worker -- --listen-addr 127.0.0.1:8082 --data-dir ./data/w3 --bootstrap-peers 127.0.0.1:8080
```

2. Test with example workflows:
```bash
cargo run --bin pulse -- submit examples/simple-ci.yml --wait
```

### Debugging

Enable detailed logging:
```bash
RUST_LOG=pulse=debug,libp2p=info cargo run --bin pulse-worker
```

Use rust-gdb for debugging:
```bash
rust-gdb target/debug/pulse-worker
```

### Performance Testing

Run benchmarks:
```bash
cargo bench
```

Profile with `perf`:
```bash
cargo build --release
perf record target/release/pulse-worker
perf report
```

## Architecture Guidelines

### Core Principles

1. **Fault Tolerance**: Components should gracefully handle failures
2. **Scalability**: Design for horizontal scaling across nodes
3. **Consistency**: Maintain data consistency across the cluster
4. **Observability**: Include logging, metrics, and tracing
5. **Security**: Validate inputs and handle secrets securely

### Adding New Features

#### New Executor Types

1. Create a new module in `pulse-executor/src/`
2. Implement the `ActionExecutor` trait
3. Register the executor in `DistributedTaskExecutor`
4. Add tests and documentation
5. Update examples if applicable

Example:
```rust
pub struct CustomExecutor;

#[async_trait]
impl ActionExecutor for CustomExecutor {
    fn can_execute(&self, action: &str) -> bool {
        action == "custom-action"
    }

    async fn execute_action(
        &self,
        action: &str,
        parameters: &HashMap<String, serde_json::Value>,
        context: &ExecutionContext,
    ) -> pulse_core::Result<TaskExecutionResult> {
        // Implementation
    }
}
```

#### New Storage Backends

1. Implement the `ReplicatedStorage` trait
2. Add configuration options
3. Update storage factory
4. Add comprehensive tests
5. Document the new backend

#### New Network Protocols

1. Add protocol handlers in `pulse-p2p`
2. Update message types
3. Implement protocol logic
4. Add integration tests
5. Update network documentation

### Error Handling

- Use `thiserror` for error types
- Provide meaningful error messages
- Include context with `anyhow` where appropriate
- Log errors at appropriate levels

```rust
#[derive(Error, Debug)]
pub enum MyError {
    #[error("Network connection failed: {0}")]
    NetworkError(String),
    
    #[error("Invalid configuration: {field}")]
    InvalidConfig { field: String },
}
```

### Async Programming

- Use `tokio` for async runtime
- Prefer `async/await` over manual futures
- Use `Arc` and `RwLock`/`Mutex` for shared state
- Handle cancellation gracefully

## Issue Guidelines

### Reporting Bugs

Include:
- Pulse version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

### Feature Requests

Include:
- Use case description
- Proposed solution
- Alternatives considered
- Implementation complexity estimate

### Security Issues

Report security vulnerabilities privately to:
security@pulse-org.example.com

## Code Review

### For Reviewers

- Focus on correctness, performance, and maintainability
- Check for proper error handling
- Verify tests cover edge cases
- Ensure documentation is updated
- Be constructive and respectful

### For Contributors

- Respond to feedback promptly
- Ask questions if feedback is unclear
- Make requested changes in separate commits
- Update the PR description as needed

## Community

### Communication Channels

- GitHub Issues: Bug reports and feature requests
- GitHub Discussions: General questions and ideas  
- Discord: Real-time chat (link in README)

### Code of Conduct

This project follows the [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct).

## Release Process

### Version Numbering

Pulse follows [Semantic Versioning](https://semver.org/):
- MAJOR: Breaking changes
- MINOR: New features (backwards compatible)
- PATCH: Bug fixes (backwards compatible)

### Release Checklist

1. Update version numbers in Cargo.toml files
2. Update CHANGELOG.md
3. Run full test suite
4. Create release branch
5. Tag the release
6. Publish to crates.io
7. Create GitHub release with notes

## Getting Help

- Read the documentation in `docs/`
- Check existing issues and discussions
- Ask questions in GitHub Discussions
- Join the community Discord

Thank you for contributing to Pulse! 🚀