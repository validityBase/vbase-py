# Python Requirements

Published package dependencies live in `requirements.in` as abstract ranges.
Do not generate a hash-locked base/runtime requirements file for package
metadata.

Human-edited terminal environment inputs live in `requirements/*.in`.
Generated hash-locked terminal environment files live in `requirements/*.txt`.

Do not edit generated `.txt` files by hand. Regenerate them with
the exact `pip-compile` command below.

Dependabot scans this flat pip-compile layout where source `.in` files and
generated `.txt` files live together. For manual security updates, use the
`Update Python Dependency Locks` workflow so `requirements.in` and/or
`requirements/*.in` are updated before locks are regenerated.

## Regenerate Locks

Install the pinned lock tooling first:

```bash
python -m pip install --require-hashes -r requirements/tools.txt
```

Regenerate terminal environment lock files:

```bash
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/dev.txt requirements/dev.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/test.txt requirements/test.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/docs.txt requirements/docs.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/tools.txt requirements/tools.in
```

## Security Dependency Updates

Run the `Update Python Dependency Locks` workflow from GitHub Actions with:

- `dependency`: package name, for example `aiohttp`
- `constraint`: lower-bound constraint, for example `>=3.14.1`
- `source_files`: requirement source files to update. For published runtime
  dependencies include `requirements.in`; for terminal environment-only
  dependencies use the default
  `requirements/dev.in requirements/test.in requirements/docs.in`

The workflow opens a pull request containing both the source `.in` changes and
the generated hash-locked `.txt` changes.
