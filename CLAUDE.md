# CLAUDE.md

## Project Overview

`agentic-data-contracts` is a Python library for YAML-first data contract governance for AI agents. It lets data engineers define what tables an agent may query, which operations are forbidden, and what resource limits apply — then enforces those rules automatically at query time.

## Tech Stack

- Python 3.12+, uv for dependency management
- Pydantic 2 for schema validation, sqlglot for SQL parsing, thefuzz for fuzzy metric search
- pytest + pytest-asyncio for testing, DuckDB for integration tests
- ruff for linting/formatting, ty for type checking
- prek for pre-commit hooks

## Project Structure

```
src/agentic_data_contracts/
├── core/          # YAML loading, Pydantic models, lightweight enforcement
├── validation/    # sqlglot checkers, Validator (Layer 1 + 2), EXPLAIN protocol
├── tools/         # 9-tool factory + middleware for Claude Agent SDK
├── semantic/      # dbt/Cube/YAML source integrations
├── adapters/      # DatabaseAdapter protocol + DuckDB implementation
└── bridge/        # Optional ai-agent-contracts compilation
```

## Common Commands

```bash
uv sync --all-extras          # Install all dependencies
uv run pytest -v              # Run all tests
uv run pytest tests/test_core # Run specific test suite
uv run ruff check src/ tests/ # Lint
uv run ruff format src/ tests/ # Format
ty check                      # Type check
prek run --all-files          # Run pre-commit hooks
```

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
- Pre-commit hooks (ruff + ty) run automatically on commit via prek
