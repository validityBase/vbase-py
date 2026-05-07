# Agent Memory

## GitHub Actions
- Third-party GitHub Actions are pinned to full commit SHAs.
- Python dependency setup uses `validityBase/vbase-github-actions/.github/actions/setup-python-deps@v1`.
- Documentation publishing delegates to `validityBase/vbase-github-actions/.github/workflows/publish-docs.yml@v1`.
- The old local `.github/actions/setup-python-deps` action was removed after callers moved to the shared action.
- `test-localhost.yml` requires `GHCR_PAT` to pull the localhost commitment service image.

