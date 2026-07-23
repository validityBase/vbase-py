# GitHub Actions

## Policy

- Third-party actions are pinned by full commit SHA for reproducibility.
- Shared vBase-owned actions and reusable workflows use `validityBase/vbase-github-actions` with reviewed release tags such as `@v1`.
- Workflow permissions are declared explicitly and kept minimal.
- Linux dependency verification workflows use generated hash-locked terminal
  environment requirements with `require-hashes`.
- Cross-platform setup checks install from source requirement ranges because the
  generated lock hashes target Linux wheels.
- Dependabot must not update generated `requirements/lock/` files directly;
  Python dependency update PRs are created through the lock update workflow so
  `requirements.in` and `requirements/src/*.in` remain authoritative.
- Secrets must come from GitHub Secrets or deployment configuration, never from committed files or logs.

## Workflows

### `.github/workflows/python-dependency-locks.yml`

- Runs on pull requests, pushes to `main`, and manual `workflow_dispatch`.
- Installs `requirements/lock/tools.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Regenerates `requirements/lock/dev.txt`, `requirements/lock/test.txt`, `requirements/lock/docs.txt`, and `requirements/lock/tools.txt`; the workflow fails if the committed lock files differ.
- Installs `requirements/lock/test.txt`, installs the package locally without dependency resolution, and runs `python -m pip check`.

### `.github/workflows/update-python-dependency-locks.yml`

- Runs manually through `workflow_dispatch`.
- Accepts an optional dependency name, version constraint, requirement source
  files, and `pip-compile --upgrade` flag.
- Updates selected requirement source files (`requirements.in` and/or
  `requirements/src/*.in`), regenerates all `requirements/lock/*.txt` files with
  pinned lock tooling, and opens a pull request when the generated dependency
  state changes.

### `.github/workflows/test-localhost.yml`

- Runs on pull requests and pushes to `main` and `dev`.
- Pulls `ghcr.io/validitybase/commitment-service-localhost:latest` using `GHCR_PAT`.
- Installs `requirements/lock/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the localhost test script and removes the commitment service container with `if: always()`.

### `.github/workflows/run-setup-matrix.yaml`

- Runs on pull requests, pushes to any branch, and manual `workflow_dispatch`.
- Uses pinned `actions/setup-python` with pip caching.
- Installs build tooling and `requirements.in` without hash checking because
  generated terminal locks target Linux wheels.
- Verifies setup across Ubuntu, macOS, and Windows for Python 3.11 and 3.12.
- Installs the package metadata in editable mode without dependency resolution,
  runs `python -m pip check`, and imports the public `vbase` package without
  Docker, secrets, or external services.

### `.github/workflows/test-forwarder-pub-dev.yml`

- Runs on pull requests and pushes to `main`.
- Installs `requirements/lock/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the forwarder tests against the public dev service using `VBASE_API_KEY`.

### `.github/workflows/update-main-docs.yml`

- Runs on pushes to `main` and manual dispatch.
- Delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Installs `requirements/lock/docs.txt` with `require-hashes: true`.
- Builds Sphinx Markdown docs into `docs/_build/markdown`.
- Publishes to the central docs repository using `DOCS_REPO_ACCESS_TOKEN`.
