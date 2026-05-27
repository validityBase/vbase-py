# Agent Memory

## Repository Purpose

`vbase-py` is the Python SDK for the validityBase platform. It provides clients,
commitment services, indexing services, typed vBase objects, datasets, and set
matching utilities for auditable data provenance workflows.

## Dependency Locking

- Runtime, development, lock-tooling, and documentation dependency inputs live in `requirements.in`, `requirements-dev.in`, `requirements-lock.in`, and `docs/requirements.in`.
- Generated lock files are `requirements.txt`, `requirements-dev.txt`, `requirements-lock.txt`, and `docs/requirements.txt`; they must include pinned versions and hashes.
- Install generated lock files with `python -m pip install --require-hashes -r <file>`.
- `pyproject.toml` defines the package's dynamic runtime dependency source; `setup.py` is only a legacy shim and should not be treated as the source of truth.
- Use the minimal pinned `pip-tools` environment from `requirements-lock.txt` before regenerating lock files.
- `.github/workflows/python-dependency-locks.yml` verifies lock freshness with the minimal lock-tooling environment.

## GitHub Actions

- Third-party GitHub Actions are pinned to full commit SHAs.
- Shared vBase-owned actions and reusable workflows use reviewed `validityBase/vbase-github-actions` version tags such as `@v1`.
- Python dependency setup uses `validityBase/vbase-github-actions/.github/actions/setup-python-deps@v1`.
- Documentation publishing delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Test and docs workflows install hashed requirements with `require-hashes`.
- `test-localhost.yml` requires `GHCR_PAT` to pull the localhost commitment service image.

## Documentation Layout

- `CLAUDE.md` is the root instruction entry point and should stay short.
- `AGENTS.md` is a thin pointer for Codex, ChatGPT coding agents, and Copilot-style agents; do not duplicate full instructions there.
- Internal specs, guides, and persistent memory live under `internal/`.
- Externally published documentation lives under `docs/`.
