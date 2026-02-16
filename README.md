# Pipely — Local CI/CD Pipeline Runner

Pipely is a lightweight CLI tool that runs CI/CD pipelines locally from a YAML definition.

It’s designed as a learning project to explore DevOps concepts including pipeline orchestration, safe command execution, and reproducible automation.

---

## ✨ Features

- Run pipelines defined in YAML
- Step-by-step execution with clear output
- Dry-run mode for safe testing
- Structured CLI using Typer
- Safe subprocess execution (no `shell=True`)

---

## 🚀 Example

### Run a pipeline

```bash
pipely run pipeline examples/pipeline.yml
```

### Example pipeline YAML

```yaml
name: demo

jobs:
  hello:
    steps:
      - name: greet
        run: echo "Hello from Pipely!"
```

---

## 🎯 Why this project exists

This project is part of my transition into DevOps and Cloud Engineering.

It demonstrates:

- Automation thinking
- Safe execution practices
- CLI tooling design
- Real-world tradeoffs in pipeline systems

This portfolio is intentionally a work in progress. Each feature is added incrementally to reflect deliberate learning and production-minded design.

---

## 🔧 Roadmap

Planned improvements:

- Parallel job execution
- Logging & timestamps
- Failure strategies (continue-on-error, retries)
- Docker-based step execution
- GitHub Actions workflow exporter

---

## 🧑‍💻 Author

Built by Luminescence Elation as part of a DevOps learning portfolio.
