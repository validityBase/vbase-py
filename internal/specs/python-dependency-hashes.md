# Python Dependency Hashes

This repository uses pip hash-checking mode for reproducible Python dependency
installs in CI and docs publishing.

Lock files are generated with Python 3.11 for CI parity. The package metadata
may still support older Python versions, but the committed locks represent the
CI install environment.

## Files

- `requirements/src/base.in` is the human-edited runtime dependency input.
- `requirements/lock/base.txt` is generated from `requirements/src/base.in` and includes pinned versions plus hashes.
- `requirements/src/dev.in` is the human-edited development dependency input.
- `requirements/lock/dev.txt` is generated from `requirements/src/dev.in` and includes runtime and development dependencies with hashes.
- `requirements/src/test.in` is the human-edited test dependency input.
- `requirements/lock/test.txt` is generated from `requirements/src/test.in` and includes test runtime dependencies with hashes.
- `requirements/src/docs.in` is the human-edited documentation dependency input.
- `requirements/lock/docs.txt` is generated from `requirements/src/docs.in` and includes documentation build dependencies with hashes.
- `requirements/src/tools.in` is the human-edited lock-regeneration tooling input.
- `requirements/lock/tools.txt` is generated from `requirements/src/tools.in` and includes the minimal `pip-tools` environment with hashes.

Do not edit generated `.txt` lock files by hand.
Runtime dependencies are configured through setuptools dynamic dependency
metadata in `pyproject.toml`, which uses `requirements/src/base.in` as the source input,
so hashed lock syntax is never passed to package metadata.

## Developer Workflow

Install pinned lock-generation tooling from the minimal lock before running
`pip-compile`. Do not bootstrap with an unpinned `pip install pip-tools`,
because a different `pip-tools` version can produce a different lockfile.

```bash
python -m pip install --require-hashes -r requirements/lock/tools.txt
```

To add or update a runtime dependency:

```bash
# edit requirements/src/base.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/base.txt requirements/src/base.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/dev.txt requirements/src/dev.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/test.txt requirements/src/test.in
```

To add or update a development dependency:

```bash
# edit requirements/src/dev.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/dev.txt requirements/src/dev.in
```

To update the lock-generation tooling, edit the pinned `pip-tools==...`
constraint in `requirements/src/tools.in`, then regenerate
`requirements/lock/tools.txt`.
Re-running `pip-compile` without changing that pin will usually produce an
identical lock file.

```bash
# edit the pip-tools==... pin in requirements/src/tools.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/tools.txt requirements/src/tools.in
```

To add or update a docs dependency:

```bash
# edit requirements/src/docs.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/docs.txt requirements/src/docs.in
```

Install local development dependencies from the generated lock:

```bash
python -m pip install --require-hashes -r requirements/lock/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

## CI Enforcement

`.github/workflows/python-dependency-locks.yml` enforces this policy on pull
requests, pushes to `main`, and manual runs. It installs the minimal
lock-generation tooling lock with `require-hashes: "true"`, regenerates all lock
files, and fails if generated files differ from committed files.
