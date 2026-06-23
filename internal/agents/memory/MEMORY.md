# Agent Memory

## Repository Purpose

`vbase-py` is the Python SDK for the validityBase platform. It provides clients,
commitment services, indexing services, typed vBase objects, datasets, and set
matching utilities for auditable data provenance workflows.

## Dependency Locking

- Published package runtime dependencies live in `requirements.in` as abstract ranges, not hash-locked pins.
- Runtime dependency ranges should include compatibility ceilings where safe so
  downstream resolvers avoid unreviewed major-version upgrades.
- Development, test, documentation, and lock-tooling dependency inputs live under `requirements/src/`.
- Generated terminal environment lock files live under `requirements/lock/`; they must include pinned versions and hashes.
- Install generated lock files with `python -m pip install --require-hashes -r <file>`.
- `pyproject.toml` defines the package's dynamic runtime dependency source; `setup.py` is only a legacy shim and should not be treated as the source of truth.
- Use the minimal pinned `pip-tools` environment from `requirements/lock/tools.txt` before regenerating lock files.
- `.github/workflows/python-dependency-locks.yml` verifies lock freshness with the minimal lock-tooling environment.

## GitHub Actions

- Third-party GitHub Actions are pinned to full commit SHAs.
- Shared vBase-owned actions and reusable workflows use reviewed `validityBase/vbase-github-actions` version tags such as `@v1`.
- Python dependency setup uses `validityBase/vbase-github-actions/.github/actions/setup-python-deps@v1`.
- Documentation publishing delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Test and docs workflows install hashed requirements with `require-hashes`.
- `.github/workflows/run-setup-matrix.yaml` installs source requirement ranges
  without hashes across Ubuntu, macOS, and Windows for Python 3.11 and 3.12.
- `test-localhost.yml` requires `GHCR_PAT` to pull the localhost commitment service image.

## Documentation Layout

- `CLAUDE.md` is the root instruction entry point and should stay short.
- `AGENTS.md` is a thin pointer for Codex, ChatGPT coding agents, and Copilot-style agents; do not duplicate full instructions there.
- Internal specs, guides, and persistent memory live under `internal/`.
- Externally published documentation lives under `docs/`.
