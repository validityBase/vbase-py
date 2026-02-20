# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**vbase-py** is a Python SDK for the validityBase (vBase) platform — a blockchain-based system for auditable data provenance. It enables verifiable records of when data was created, by whom, and how it has changed. Key use cases include point-in-time datasets, auditable investment track records, and verifiable backtests.

## Commands

### Setup

```bash
python -m pip install -e .
pip install -r requirements-dev.txt
pre-commit install
```

### Linting & Formatting

```bash
black vbase/                  # Format code
isort vbase/                  # Sort imports (skips vbase/__init__.py by design)
pylint vbase/                 # Lint
pre-commit run --all-files    # Run all hooks
```

### Running Tests

Tests require a running Ethereum node or forwarder. Environment variables must be set first.

```bash
# Full localhost test suite (requires Docker + Ethereum test node on port 8545)
./vbase/tests/scripts/run_tests_localhost.sh

# Manual: source env, then run specific test file
source config/.env.localhost
python3 -m unittest vbase/tests/test_vbase_client.py

# Run all tests via discovery
source config/.env.localhost
python3 -m unittest discover -s vbase/tests
```

### Documentation

```bash
cd docs && make markdown      # Build Sphinx docs to docs/_build/markdown/
```

## Architecture

The SDK is organized in layers:

**Layer 1 — Commitment Services** (`vbase/core/commitment_service.py`): Abstract base (`CommitmentService`) for writing commitments to the blockchain. Concrete implementations:
- `Web3HTTPCommitmentService` — Direct HTTP connection to an Ethereum-compatible node
- `ForwarderCommitmentService` — Proxied through a remote forwarder (no direct blockchain key needed)
- `*TestCommitmentService` variants — Deterministic behavior for testing

**Layer 2 — Indexing Services** (`vbase/core/indexing_service.py`): Abstract base for querying past commitments. Implementations:
- `Web3HTTPIndexingService` — Direct chain queries
- `SQLIndexingService` — SQL-backed with matching service support
- `AggregateIndexingService` — Combines multiple services
- `FailoverIndexingService` — Fallback chain

**Layer 3 — Client** (`vbase/core/vbase_client.py`): `VBaseClient` is the main entry point for user code. Wraps commitment + indexing services.

**Layer 4 — Data Objects** (`vbase/core/vbase_object.py`): `VBaseObject` (abstract) with typed subclasses: `VBaseIntObject`, `VBaseFloatObject`, `VBaseStringObject`, `VBaseJsonObject`, `VBasePortfolioObject`, `VBaseBytesObject`, and `VBasePrivate*` variants.

**Layer 5 — Datasets** (`vbase/core/vbase_dataset.py`): `VBaseDataset` manages collections of objects with provenance. `VBaseDatasetAsync` provides non-blocking operations.

**Layer 6 — Set Matching** (`vbase/core/set_matching_service.py`): Reverse-lookup service that answers the question "which previously-committed on-chain dataset best matches this list of (object_cid, timestamp) pairs?" This identifies if any on-chain commitments match a given dataset without any hints as to the address or collection name. If there is a match, it can be found. `SQLIndexingService.find_matching_user_sets()` is the public entry point; it delegates to a `BaseMatchingService` instance. `SetMatchingService` is the SQL-backed default implementation, scoring candidates by the fraction of query objects that have a committed counterpart within a configurable timestamp tolerance (`SetMatchingServiceConfig.max_timestamp_diff`, default 1 day). Pass a custom `BaseMatchingService` subclass to `SQLIndexingService(matching_service=...)` to override the strategy. Config and shared types (`ObjectAtTime`, `SetCandidate`, `SetMatchingCriteria`, `SetMatchingServiceConfig`) live in `vbase/core/types.py`.

Smart contract ABIs are stored in `vbase/core/abi/`.

## Environment Configuration

Tests use `.env` files in `config/`:
- `config/.env.localhost` — Local Ethereum test node (port 8545)
- `config/.env.forwarder.localhost` — Local forwarder proxy
- `config/.env.forwarder.pub.dev` — Public dev forwarder

Key env vars: `VBASE_COMMITMENT_SERVICE_NODE_RPC_URL`, `VBASE_COMMITMENT_SERVICE_ADDRESS`, `VBASE_COMMITMENT_SERVICE_PRIVATE_KEY`.

## Code Conventions

- **Formatting**: Black + isort (Black-compatible profile). Run pre-commit hooks before committing.
- **`vbase/__init__.py`**: Import order is intentional (logical, not alphabetical) — isort skips this file.
- **Pylint short names**: Single-letter variables `cl, cr, cs, e, f, i, j, k, n, s, t, ts, tx, x, y, w3` are allowed.
- **Python version**: 3.11.9 (see `.python-version`), supports 3.8+.
- **PEP8**: Conform to PEP8. Keep all imports at the top of each file.
- **Type safety**: Use type hints throughout. Never use plain dicts, tuples, or other untyped collections to pass or return complex data — use `TypedDict` or typed dataclasses instead.
- **File I/O**: Always specify `encoding=` explicitly when calling `open()`.
- **Scope of changes**: Make strictly isolated changes. Do not modify unrelated code, comments, type annotations, or settings.
- **Minimal implementation**: Use the minimum implementation needed. Do not add unnecessary abstraction layers or single-use one-line helper functions.
- **Standard library first**: Always check whether a standard library or existing API solves the problem before writing custom code (e.g., use `python-dotenv` rather than a custom `.env` parser).
- **DRY**: Factor duplicated code into shared utilities. Avoid large or complex functions — split them into small, focused units.
- **Comments**: Place comments on the line above the relevant code, not inline, to keep lines short.
- **Linter warnings**: Fix warnings by addressing the root cause. Do not suppress or disable them.
- **Tests**: Use the `unittest` package for all unit tests.
- **Virtual environment**: Run code using the virtual environment in the `venv/` folder.

## CI/CD

Three GitHub Actions workflows:
- `test-localhost.yml` — Pulls `ghcr.io/validitybase/commitment-service-localhost:latest` Docker image, runs Ethereum test node, runs localhost test suite. Triggers on PRs and pushes to `main`/`dev`.
- `test-forwarder-pub-dev.yml` — Same flow against public forwarder service.
- `update-main-docs.yml` — Builds and publishes Sphinx docs. Requires `DOCS_REPO_ACCESS_TOKEN` secret.