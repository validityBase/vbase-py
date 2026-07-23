# Python Dependency Hashes

This repository separates published package dependencies from terminal
environment locks.

`vbase-py` is an intermediate library installed into downstream applications, so
published runtime dependencies must stay abstract and resolver-friendly. Prefer
lower bounds plus compatibility ceilings where safe, and reserve exact
resolutions for terminal environment locks. CI, tests, docs publishing, and lock
tooling are terminal environments owned by this repo, so those installs use pip
hash-checking mode for reproducibility.

Lock files are generated with Python 3.11 for CI parity. The package metadata
may still support older Python versions, but the committed locks represent the
CI install environment.

## Files

- `requirements.in` is the human-edited published runtime dependency source.
  It is read by `pyproject.toml` and must use dependency ranges rather than
  hash-locked pins.
- `requirements/dev.in` is the human-edited development environment input.
- `requirements/dev.txt` is generated from `requirements/dev.in` and
  includes runtime and development dependencies with hashes.
- `requirements/test.in` is the human-edited test environment input.
- `requirements/test.txt` is generated from `requirements/test.in` and
  includes test runtime dependencies with hashes.
- `requirements/docs.in` is the human-edited documentation publishing input.
- `requirements/docs.txt` is generated from `requirements/docs.in` and
  includes package runtime and documentation build dependencies with hashes.
- `requirements/tools.in` is the human-edited lock-regeneration tooling
  input.
- `requirements/tools.txt` is generated from `requirements/tools.in`
  and includes the minimal `pip-tools` environment with hashes.

Do not create a generated base/runtime lock for package metadata. Do not edit
generated `.txt` lock files by hand.

## Developer Workflow

Install pinned lock-generation tooling from the minimal lock before running
`pip-compile`. Do not bootstrap with an unpinned `pip install pip-tools`,
because a different `pip-tools` version can produce a different lockfile.

```bash
python -m pip install --require-hashes -r requirements/tools.txt
```

To add or update a published runtime dependency:

```bash
# edit requirements.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/dev.txt requirements/dev.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/test.txt requirements/test.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/docs.txt requirements/docs.in
```

To add or update a development dependency:

```bash
# edit requirements/dev.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/dev.txt requirements/dev.in
```

To add or update a docs dependency:

```bash
# edit requirements/docs.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/docs.txt requirements/docs.in
```

To update the lock-generation tooling, edit the pinned `pip-tools==...`
constraint in `requirements/tools.in`, then regenerate
`requirements/tools.txt`.

```bash
# edit the pip-tools==... pin in requirements/tools.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/tools.txt requirements/tools.in
```

Install local development dependencies from the generated lock:

```bash
python -m pip install --require-hashes -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

## CI Enforcement

`.github/workflows/python-dependency-locks.yml` enforces this policy on pull
requests, pushes to `main`, and manual runs. It installs the minimal
lock-generation tooling lock with `require-hashes: "true"`, regenerates terminal
environment lock files, fails if generated files differ from committed files,
then installs the test lock, installs the package locally without dependency
resolution, and runs `python -m pip check`.

`.github/workflows/run-setup-matrix.yaml` complements the Linux hash-locked
checks by installing the published runtime ranges from `requirements.in` without
hash checking across Ubuntu, macOS, and Windows for Python 3.11 and 3.12. This
workflow validates resolver compatibility and package importability on supported
setup targets without Docker, secrets, or external services.
