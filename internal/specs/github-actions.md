# GitHub Actions

## Policy

- Third-party actions are pinned by full commit SHA for reproducibility.
- Shared vBase-owned actions and reusable workflows use `validityBase/vbase-github-actions` with reviewed release tags such as `@v1`.
- Workflow permissions are declared explicitly and kept minimal.
- Python installs use generated hash-locked requirements with `require-hashes` wherever requirements are installed.
- Secrets must come from GitHub Secrets or deployment configuration, never from committed files or logs.

## Workflows

### `.github/workflows/python-dependency-locks.yml`

- Runs on pull requests, pushes to `main`, and manual `workflow_dispatch`.
- Installs `requirements-dev.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Regenerates `requirements.txt`, `requirements-dev.txt`, and `docs/requirements.txt`; the workflow fails if the committed lock files differ.
- Installs the package locally with `python -m pip install --no-deps --no-build-isolation -e .`.
- Runs `python -m pip check`.

### `.github/workflows/test-localhost.yml`

- Runs on pull requests and pushes to `main` and `dev`.
- Pulls `ghcr.io/validitybase/commitment-service-localhost:latest` using `GHCR_PAT`.
- Installs `requirements.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the localhost test script and removes the commitment service container with `if: always()`.

### `.github/workflows/test-forwarder-pub-dev.yml`

- Runs on pull requests and pushes to `main`.
- Installs `requirements.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the forwarder tests against the public dev service using `VBASE_API_KEY`.

### `.github/workflows/update-main-docs.yml`

- Runs on pushes to `main` and manual dispatch.
- Delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Installs `docs/requirements.txt` before `requirements.txt` with `require-hashes: true`.
- Builds Sphinx Markdown docs into `docs/_build/markdown`.
- Publishes to the central docs repository using `DOCS_REPO_ACCESS_TOKEN`.
