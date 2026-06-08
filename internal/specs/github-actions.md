# GitHub Actions

## Policy

- Third-party actions are pinned by full commit SHA for reproducibility.
- Shared vBase-owned actions and reusable workflows use `validityBase/vbase-github-actions` with reviewed release tags such as `@v1`.
- Workflow permissions are declared explicitly and kept minimal.
- Python installs for CI, tests, and docs use generated hash-locked terminal environment requirements with `require-hashes`.
- Secrets must come from GitHub Secrets or deployment configuration, never from committed files or logs.

## Workflows

### `.github/workflows/python-dependency-locks.yml`

- Runs on pull requests, pushes to `main`, and manual `workflow_dispatch`.
- Installs `requirements/lock/tools.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Regenerates `requirements/lock/dev.txt`, `requirements/lock/test.txt`, `requirements/lock/docs.txt`, and `requirements/lock/tools.txt`; the workflow fails if the committed lock files differ.
- Installs `requirements/lock/test.txt`, installs the package locally without dependency resolution, and runs `python -m pip check`.

### `.github/workflows/test-localhost.yml`

- Runs on pull requests and pushes to `main` and `dev`.
- Pulls `ghcr.io/validitybase/commitment-service-localhost:latest` using `GHCR_PAT`.
- Installs `requirements/lock/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the localhost test script and removes the commitment service container with `if: always()`.

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
