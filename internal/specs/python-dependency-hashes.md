# Python Dependency Hashes

This repository uses pip hash-checking mode for reproducible Python dependency
installs in CI and docs publishing.

Lock files are generated with Python 3.11 for CI parity. The package metadata
may still support older Python versions, but the committed locks represent the
CI install environment.

## Files

- `requirements.in` is the human-edited runtime dependency input.
- `requirements.txt` is generated from `requirements.in` and includes pinned versions plus hashes.
- `requirements-dev.in` is the human-edited development dependency input.
- `requirements-dev.txt` is generated from `requirements-dev.in` and includes runtime and development dependencies with hashes.
- `requirements-lock.in` is the human-edited lock-regeneration tooling input.
- `requirements-lock.txt` is generated from `requirements-lock.in` and includes the minimal `pip-tools` environment with hashes.
- `docs/requirements.in` is the human-edited documentation dependency input.
- `docs/requirements.txt` is generated from `docs/requirements.in` and includes documentation build dependencies with hashes.

Do not edit generated `.txt` lock files by hand.
Runtime dependencies are configured through setuptools dynamic dependency
metadata in `pyproject.toml`, which uses `requirements.in` as the source input,
so hashed lock syntax is never passed to package metadata.

## Developer Workflow

Install pinned lock-generation tooling from the minimal lock before running
`pip-compile`. Do not bootstrap with an unpinned `pip install pip-tools`,
because a different `pip-tools` version can produce a different lockfile.

```bash
python -m pip install --require-hashes -r requirements-lock.txt
```

To add or update a runtime dependency:

```bash
# edit requirements.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements.txt requirements.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements-dev.txt requirements-dev.in
```

To add or update a development dependency:

```bash
# edit requirements-dev.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements-dev.txt requirements-dev.in
```

To update the lock-generation tooling, edit the pinned `pip-tools==...`
constraint in `requirements-lock.in`, then regenerate `requirements-lock.txt`.
Re-running `pip-compile` without changing that pin will usually produce an
identical lock file.

```bash
# edit the pip-tools==... pin in requirements-lock.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements-lock.txt requirements-lock.in
```

To add or update a docs dependency:

```bash
# edit docs/requirements.in
pip-compile --strip-extras --no-annotate --generate-hashes -o docs/requirements.txt docs/requirements.in
```

Install local development dependencies from the generated lock:

```bash
python -m pip install --require-hashes -r requirements-dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

## CI Enforcement

`.github/workflows/python-dependency-locks.yml` enforces this policy on pull
requests, pushes to `main`, and manual runs. It installs the minimal
lock-generation tooling lock with `require-hashes: "true"`, regenerates all lock
files, and fails if generated files differ from committed files.
