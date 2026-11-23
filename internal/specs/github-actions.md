# GitHub Actions

## Policy

- Third-party actions are pinned by full commit SHA for reproducibility.
- Shared vBase-owned release workflows and composite actions are pinned by full
  commit SHA; each release-tools-ref matches its workflow SHA. Non-release
  workflows may use reviewed shared release tags such as `@v1`.
- Workflow permissions are declared explicitly and kept minimal.
- Linux dependency verification workflows use generated hash-locked terminal
  environment requirements with `require-hashes`.
- Cross-platform setup checks install from source requirement ranges because the
  generated locks are resolved on Linux and omit dependencies that only macOS
  or Windows need.
- Secrets must come from GitHub Secrets or deployment configuration, never from committed files or logs.

## Workflows

### `.github/workflows/python-dependency-locks.yml`

- Runs on pull requests, pushes to `main`, and manual `workflow_dispatch`.
- Installs `requirements/tools.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Regenerates `requirements/dev.txt`, `requirements/test.txt`, `requirements/docs.txt`, and `requirements/tools.txt`; the workflow fails if the committed lock files differ.
- Installs `requirements/test.txt`, installs the package locally without dependency resolution, and runs `python -m pip check`.

### `.github/workflows/run-pylint.yaml`

- Runs on every pull request, including pull requests into `dev`, and on
  pushes to `main`.
- Installs `requirements/dev.txt` through `setup-python-deps@v1` with Python
  3.11 and `require-hashes: "true"`.
- Runs `pylint vbase/`.

### `.github/workflows/test-localhost.yml`

- Runs on every pull request, including pull requests into `dev`, and on
  pushes to `main`.
- Pulls `ghcr.io/validitybase/commitment-service-localhost:latest` using the workflow `GITHUB_TOKEN` with `packages: read`.
- Installs `requirements/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the localhost test script and removes the commitment service container with `if: always()`.
- These are direct-node tests. Localhost forwarder tests run in the
  forwarder service's CI; see [Localhost Forwarder Tests](development.md#localhost-forwarder-tests)
  for the port and indexing configuration.

### `.github/workflows/run-setup-matrix.yaml`

- Runs when a pull request is opened, updated with new commits, or reopened.
  It can also be started manually with `workflow_dispatch`.
- Uses pinned `actions/setup-python` with pip caching.
- Installs build tooling and `requirements.in` without hash checking because
  generated locks are resolved on Linux and omit dependencies that only macOS
  or Windows need.
- Verifies setup across Ubuntu, macOS, and Windows for Python 3.11 through 3.13.
- Installs the package metadata in editable mode without dependency resolution,
  runs `python -m pip check`, and imports the public `vbase` package without
  Docker, secrets, or external services.

### `.github/workflows/test-forwarder-pub-dev.yml`

- Runs on pushes to `main` and manual `workflow_dispatch` on `main`; it does
  not run on pull requests because the job requires a live dev-service API key.
- Installs `requirements/test.txt` through `setup-python-deps@v1` with Python 3.11 and `require-hashes: "true"`.
- Runs the forwarder tests against the public dev service using `VBASE_API_KEY`
  and `VBASE_TEST_INDEXING_NODE_RPC_URL` from GitHub Actions secrets. See
  [Public Dev Tests](development.md#public-dev-tests) for configuration.
- Labels the package source as `source` so the shared test runner verifies that
  the checkout package is imported.
- The shared runner creates a fresh ephemeral signer for each invocation, so no
  signer private key needs to be stored as a CI secret.
- Uses a workflow-level concurrency group with `cancel-in-progress: false` so
  source and PyPI runs do not overload the same public dev forwarder account.
  Different runs have different signer addresses, but still share the API key
  and account-level limits.

### `.github/workflows/test-forwarder-pub-dev-pypi.yml`

- Runs daily on the schedule in the workflow file and supports manual
  `workflow_dispatch` on `main`;
  it does not run on pull requests because the job requires a live dev-service
  API key.
- Uses a sequential Ubuntu, macOS, and Windows matrix to test the latest
  published `vbase` package from PyPI against the public dev forwarder.
- Uses the shared `setup-python-deps@v1` action to install the cross-platform
  `requirements/test.in` input with `require-hashes: false`, then runs
  `python -m pip install --upgrade vbase` so the job exercises the package
  users install rather than the repository checkout.
- Runs the same `test_vbase_client` and `test_indexing_service` modules as the
  source-install forwarder workflow. The runner loads those test modules from
  the checkout while keeping the imported `vbase` package in site-packages.
- Because the tests come from `main` and the package from PyPI, a test on
  `main` that needs an unreleased SDK change fails this workflow until that
  change is published.
- Uses the same API key and indexing RPC secrets as the source workflow;
  the shared runner generates a fresh signer for each matrix leg.
- Shares the source-install workflow's concurrency group because all matrix
  legs use the same API key and account-level limits.

### `.github/workflows/update-main-docs.yml`

- Runs on pushes to `main` and manual dispatch.
- Delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- Installs `requirements/docs.txt` with `require-hashes: true`.
- Builds Sphinx Markdown docs into `docs/_build/markdown`.
- Publishes to the central docs repository using `DOCS_REPO_ACCESS_TOKEN`.

### Python release workflow wrappers

- `prepare-release.yml` calls the shared `prepare-python-release.yml` with an
  explicit patch/minor/major increment and `version-file: vbase/_version.py`.
- `tag-release.yml` calls `tag-python-release.yml` for merged release PRs;
  ordinary, unmerged, and fork PRs do not publish.
- `publish-pypi.yml` calls `build-python-release.yml` with `package-name: vbase`,
  downloads the validated artifact, and invokes the pinned PyPA publisher
  directly in its own `publish-to-pypi` job, which uses the `pypi` environment.
  It calls `github-python-release.yml` after upload succeeds.
- Manual dispatch accepts an optional `release-tag` and forwards it to both
  build and GitHub Release creation. An explicit tag requires dispatch from
  `main`; the selected full tag ref shares a concurrency lock with tag runs.
- The shared workflow SHA and release-tools-ref match. Scripts and tests live
  in the shared-actions repository. Keep the upload workflow filename, PyPI
  project, environment, and trusted-publisher identity unchanged unless the
  PyPI trusted publisher is updated to match.
- The runbook is [RELEASE_MANAGEMENT.md](../../RELEASE_MANAGEMENT.md); repository
  settings and behavior are in [pypi-releases.md](pypi-releases.md).
