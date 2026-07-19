# CLAUDE.md

## Project Overview

`agentic-data-contracts` is a Python library for YAML-first data contract governance for AI agents. It lets data engineers define what tables an agent may query, which operations are forbidden, and what resource limits apply — then enforces those rules automatically at query time.

## Common Commands

```bash
uv sync --all-extras          # Install all dependencies
uv run pytest -v              # Run all tests
uv run pytest tests/test_core # Run specific test suite
prek run --all-files          # Lint + format + type check (what CI runs)
prek run ty --all-files       # Type check only
prek run ruff-check --all-files  # Lint only
```

Run the linters **through prek**, not directly. The hook `rev`s in
`.pre-commit-config.yaml` pin ruff and ty, and CI runs those same hooks — so
`prek run` reproduces CI exactly, while a bare `ty check` or `uv run ruff` uses
whatever version happens to be on PATH or in the lockfile.

## Key Design Decisions

- **Optional `ai-agent-contracts` dependency**: Library works standalone with lightweight enforcement; `ai-agent-contracts` upgrades to formal 7-tuple Contract model
- **Protocol-based extensibility**: `DatabaseAdapter`, `SemanticSource`, `ExplainAdapter`, and `Checker` are all `@runtime_checkable` protocols
- **Two-layer validation**: Layer 1 (sqlglot static analysis) always runs; Layer 2 (EXPLAIN dry-run) runs when a database adapter is available
- **Tools are plain async functions**: Compatible with Claude Agent SDK via `create_sdk_mcp_server()` but framework-agnostic

## Conventions

- Follow TDD: write tests first, then implement
- Each layer is independently testable with its own test suite under `tests/test_<layer>/`
- YAML fixtures live in `tests/fixtures/`
- Use `uv run` to execute anything Python-related
- Pre-commit hooks (ruff + ty) run automatically on commit via prek, and CI runs the same hooks — never name a tool version outside `.pre-commit-config.yaml`, or it will drift from the hook. Dependabot bumps the `rev`s weekly.
