# GitHub Actions

## Policy

- Third-party actions are pinned by full commit SHA for reproducibility.
- Shared vBase-owned actions and reusable workflows use `validityBase/vbase-github-actions` with reviewed release tags such as `@v1`.
- Workflow permissions are declared explicitly and kept minimal.
- Linux dependency verification workflows use generated hash-locked terminal
  environment requirements with `require-hashes`.
- Cross-platform setup checks install from source requirement ranges because the
  generated lock hashes target Linux wheels.
- Secrets must come from GitHub Secrets or deployment configuration, never from committed files or logs.

## Workflows

### `.github/workflows/python-dependency-locks.yml`

- Runs on pull requests, pushes to `main`, and manual `workflow_dispatch`.
- Installs `requirements/tools.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Regenerates `requirements/dev.txt`, `requirements/test.txt`, `requirements/docs.txt`, and `requirements/tools.txt`; the workflow fails if the committed lock files differ.
- Installs `requirements/test.txt`, installs the package locally without dependency resolution, and runs `python -m pip check`.

### `.github/workflows/test-localhost.yml`

- Runs on pull requests and pushes to `main` and `dev`.
- Pulls `ghcr.io/validitybase/commitment-service-localhost:latest` using the workflow `GITHUB_TOKEN` with `packages: read`.
- Installs `requirements/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
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
- Installs `requirements/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the forwarder tests against the public dev service using the GitHub
  Actions secrets `VBASE_API_KEY` and `VBASE_COMMITMENT_SERVICE_PRIVATE_KEY`.
- Uses a workflow-level concurrency group with `cancel-in-progress: false` so
  independent pull request and `main` push runs do not execute against the same
  public dev forwarder account at the same time. Serial execution avoids
  nonce/signature races when multiple runs share `VBASE_API_KEY`.

### `.github/workflows/update-main-docs.yml`

- Runs on pushes to `main` and manual dispatch.
- Delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Installs `requirements/docs.txt` with `require-hashes: true`.
- Builds Sphinx Markdown docs into `docs/_build/markdown`.
- Publishes to the central docs repository using `DOCS_REPO_ACCESS_TOKEN`.

### `.github/workflows/repo-backup.yml`

- Runs daily at 02:17 UTC and can be triggered manually.
- Delegates to `validityBase/vbase-github-actions/.github/workflows/repo-backup.yml@v1`.
- Uses the reviewed moving major tag for validityBase-owned shared workflows so centrally reviewed fixes roll forward without per-repository pin updates.
- Creates a full-history git bundle, checksum, and metadata file under the shared `github-backups` object storage prefix.
- Passes `VBASE_COMMON_REPO_READ_TOKEN` and maps `VBASE_REPO_BACKUP_SECRETS_TOKEN` to the shared workflow's `BWS_ACCESS_TOKEN`.
- Reads object storage credentials from the `vbase-repo-backups` Bitwarden project instead of storing provider credentials directly in GitHub Secrets.
