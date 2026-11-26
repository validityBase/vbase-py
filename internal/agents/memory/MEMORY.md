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

- Workflow policy, shared action references, and documentation publishing are
  in `internal/specs/github-actions.md`.
- Test and docs workflows install hashed requirements with `require-hashes`,
  except the PyPI public-dev workflow, which installs `requirements/test.in`
  without hashes; see `internal/specs/github-actions.md`.
- `.github/workflows/run-setup-matrix.yaml` installs source requirement ranges
  without hashes across Ubuntu, macOS, and Windows for Python 3.11 through 3.13.
- CI access to the localhost image is in `internal/specs/github-actions.md`;
  local access for validityBase members is in `CONTRIBUTING.md`.

## Sensitive Logging

- URL redaction rules and their by-design limits are canonical in the
  Sensitive Logging section of `internal/specs/development.md`, including
  "URL Redaction Limits". `.github/instructions/url-redaction.instructions.md`
  is a short copy of the limits for Copilot code review; update both together.

## Documentation Layout

- `CLAUDE.md` is the root instruction entry point and should stay short.
- `AGENTS.md` is a thin pointer for Codex, ChatGPT coding agents, and Copilot-style agents; do not duplicate full instructions there.
- Internal specs, guides, and persistent memory live under `internal/`.
- Externally published documentation lives under `docs/`.
