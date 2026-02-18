# Pipely — Local CI/CD Pipeline Runner

Pipely is a lightweight CLI tool that runs CI/CD pipelines locally from a YAML definition.

It’s designed as a learning project to explore DevOps concepts including pipeline orchestration, safe command execution, parallelism, and failure handling.

---

## Features

- Run pipelines defined in YAML
- Step-by-step execution with clear output
- Dry-run mode for safe testing
- Structured CLI using Typer
- Safe subprocess execution (no `shell=True`)
- Parallel job execution with worker limits
- Dependency-aware scheduling (`needs`)
- Retries, timeouts, and failure strategies
- Fail-fast pipeline cancellation
- Job timing and summary output

---

## Example

### Run a pipeline

```bash
pipely run pipeline examples/pipeline.yml
```

### Run with parallel workers

```bash
pipely run pipeline examples/pipeline.yml --max-workers 2
```

### Run with fail-fast enabled

```bash
pipely run pipeline examples/fail_fast.yml --fail-fast
```

---

## Example pipeline YAML

```yaml
name: demo

jobs:
  hello:
    steps:
      - name: greet
        run: echo "Hello from Pipely!"
```

---

## Why this project exists

This project is part of my transition into DevOps and Cloud Engineering.

It demonstrates:

- Automation thinking
- Safe execution practices
- CLI tooling design
- Real-world tradeoffs in pipeline systems
- Parallel orchestration & dependency management

This portfolio is intentionally a work in progress. Each feature is added incrementally to reflect deliberate learning and production-minded design.

---

## Roadmap

Planned improvements:

- Docker-based step execution
- GitHub Actions workflow exporter
- Remote runners
- Plugin system for custom executors

---

## Author

Built by **Luminescence Elation** as part of a DevOps learning portfolio.