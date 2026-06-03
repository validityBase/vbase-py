# Python Requirements

Human-edited dependency inputs live in `requirements/src/`.
Generated hash-locked files live in `requirements/lock/`.

Do not edit files in `requirements/lock/` by hand. Regenerate them with
`pip-compile --generate-hashes`.

## Regenerate Locks

Install the pinned lock tooling first:

```bash
python -m pip install --require-hashes -r requirements/lock/tools.txt
```

Regenerate all lock files:

```bash
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/base.txt requirements/src/base.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/dev.txt requirements/src/dev.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/test.txt requirements/src/test.in
pip-compile --strip-extras --no-annotate --generate-hashes -o requirements/lock/docs.txt requirements/src/docs.in
pip-compile --strip-extras --no-annotate --allow-unsafe --generate-hashes -o requirements/lock/tools.txt requirements/src/tools.in
```
