# CLAUDE.md

This file is the root entry point for agentic coding work in this repository.
Keep it short and move detailed, workflow-specific context into `internal/`.

## Core Standards

- Python code follows Black, isort, PEP8, and pylint conventions.
- Use `unittest` for tests.
- Keep imports at the top of each file.
- Use explicit `encoding=` for file I/O.
- Use type hints for new or changed APIs.
- Keep changes scoped and avoid unrelated refactors.
- Do not commit secrets, tokens, webhook URLs, generated `.env` payloads, or private credentials.

## Common Commands

```bash
python -m pip install --require-hashes -r requirements-dev.txt
python -m pip install --no-deps --no-build-isolation -e .
pre-commit run --all-files
```

Tests require a configured commitment service or forwarder:

```bash
./vbase/tests/scripts/run_tests_localhost.sh
./vbase/tests/scripts/run_tests_forwarder_pub_dev.sh
```

## Internal Documentation

- Agent memory: [internal/agents/memory/MEMORY.md](internal/agents/memory/MEMORY.md)
- Development guide: [internal/specs/development.md](internal/specs/development.md)
- Architecture notes: [internal/specs/app-architecture.md](internal/specs/app-architecture.md)
- GitHub Actions spec: [internal/specs/github-actions.md](internal/specs/github-actions.md)
- Dependency hashes: [internal/specs/python-dependency-hashes.md](internal/specs/python-dependency-hashes.md)

Externally published documentation lives in `docs/`.
