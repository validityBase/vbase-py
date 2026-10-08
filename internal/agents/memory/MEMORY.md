# Agent Memory

## Repository Purpose

`vbase-py` is the Python SDK for the validityBase platform. It provides clients,
commitment services, indexing services, typed vBase objects, datasets, and set
matching utilities for auditable data provenance workflows.

## Dependency Notes

- Dependency layout, lock policy, and package metadata rules are canonical in
  `internal/specs/python-dependency-hashes.md`; keep that as the only detailed
  copy.

## GitHub Actions

- Third-party GitHub Actions are pinned to full commit SHAs.
- Shared vBase-owned actions and reusable workflows use reviewed `validityBase/vbase-github-actions` version tags such as `@v1`.
- Python dependency setup uses `validityBase/vbase-github-actions/.github/actions/setup-python-deps@v1`.
- Documentation publishing delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Test and docs workflows install hashed requirements with `require-hashes`.
- `.github/workflows/run-setup-matrix.yaml` installs source requirement ranges
  without hashes across Ubuntu, macOS, and Windows for Python 3.11 and 3.12.
- `test-localhost.yml` uses the workflow `GITHUB_TOKEN` with `packages: read` to pull the localhost commitment service image.
- Repository backups use `.github/workflows/repo-backup.yml`, which delegates
  to the shared `repo-backup.yml@v1` workflow and resolves generic object
  storage credentials from the `vbase-repo-backups` Bitwarden project.

## Documentation Layout

- `CLAUDE.md` is the root instruction entry point and should stay short.
- `AGENTS.md` is a thin pointer for Codex, ChatGPT coding agents, and Copilot-style agents; do not duplicate full instructions there.
- Internal specs, guides, and persistent memory live under `internal/`.
- Externally published documentation lives under `docs/`.
