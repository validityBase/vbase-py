# Python Requirements

Published package dependencies live in `../requirements.in` as abstract ranges.
Do not generate a hash-locked base/runtime requirements file for package
metadata.

Human-edited terminal environment inputs live in `requirements/src/`.
Generated hash-locked terminal environment files live in `requirements/lock/`.

Do not edit files in `requirements/lock/` by hand. Regenerate them with
the exact `pip-compile` command below.

Dependabot must not edit generated files in `requirements/lock/` directly. For
security updates, use the `Update Python Dependency Locks` workflow so
`requirements.in` and/or `requirements/src/*.in` are updated before locks are
regenerated.

## Regenerate Locks

Install the pinned lock tooling first:

```bash
python -m pip install --require-hashes -r requirements/lock/tools.txt
```

Regenerate terminal environment lock files:

```bash
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/dev.txt requirements/src/dev.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/test.txt requirements/src/test.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/docs.txt requirements/src/docs.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/tools.txt requirements/src/tools.in
```

## Security Dependency Updates

Run the `Update Python Dependency Locks` workflow from GitHub Actions with:

- `dependency`: package name, for example `aiohttp`
- `constraint`: lower-bound constraint, for example `>=3.14.1`
- `source_files`: requirement source files to update. For published runtime
  dependencies include `requirements.in`; for terminal environment-only
  dependencies use the default
  `requirements/src/dev.in requirements/src/test.in requirements/src/docs.in`

The workflow opens a pull request containing both the source `.in` changes and
the generated hash-locked `.txt` changes.
