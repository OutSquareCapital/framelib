# Contributing to framelib

Thank you for your interest in contributing to framelib! We welcome contributions from the community.
Please follow the guidelines below to set up your development environment and contribute effectively.

## Installation

Once cloned, install the development dependencies using `uv`:

```bash
uv sync --dev
```

## Type Checking

So far, framelib has been developped with Pylance in strict mode, with the config directly in IDE settings.

It is yet to be determined how to best integrate type checking in the CI pipeline.

**Note:**
    Narhwals and Polars currently have numpy types in their methods, which cause issues in environnements where numpy is not installed (like this one).
    Current solution involve manually deleting the type (IntoExpr union for example) in the venv code to avoid numpy types.

## Running Tests and Linters

Before submitting a pull request, ensure that all tests pass and that the code adheres to the project's style guidelines.

You can do this by running the following commands:

```bash
uv run pydoclint src/framelib
uv run ruff check --fix --unsafe-fixes src/framelib
uv run ruff format src/framelib
uv run pytest tests/
```
