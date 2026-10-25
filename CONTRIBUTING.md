# Contributing to the vBase Python Software Development Kit (SDK)

Thank you for considering contributing to the vBase Python Software Development Kit (SDK)!

# How to Contribute

## Reporting Issues

If you encounter a bug or have a feature request,
please use the GitHub Issues section of the repository to report it:
- Check Existing Issues:
Make sure the issue has not already been reported or addressed.
- Create a New Issue:
If your issue is new, click the "New Issue" button and select the appropriate template
(bug report or feature request).
- Fill Out the Template:
Provide as much information as possible to help us understand the problem or feature.

## Making Contributions

To contribute code or documentation, please do the following:
- Create a fork of the repository.
- Create a new branch with your change, and push the changes to it.
- Submit a pull request for your change.
Provide a detailed description of the changes and any supporting information.

## Development Setup

From your cloned repository, use Python 3.11 (the version used to generate CI
locks) and create a virtual environment. The activation command is for
macOS/Linux; on Windows PowerShell use `venv\Scripts\Activate.ps1`.

```bash
python -m venv venv
source venv/bin/activate
python -m pip install --require-hashes -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
pre-commit install
```

The generated locks target Linux wheels. For local development on macOS or
Windows, replace the requirements install with
`python -m pip install -r requirements/dev.in`.

Check your environment and run formatting/lint checks:

```bash
python -m pip check
python -c "import vbase"
pre-commit run --all-files
pylint vbase/
```

Tests use `unittest` and require a configured commitment service. With a
localhost service running and `config/.env.localhost` configured, run:

```bash
bash vbase/tests/scripts/run_tests_localhost.sh
```

Alternatively, with authorized access to the public development forwarder and
`VBASE_API_KEY` supplied privately in your environment, run:

```bash
bash .github/scripts/run_tests_forwarder_pub_dev.sh
```

These tests make service calls; installing the package alone does not provision
a service. See the [development guide](internal/specs/development.md) for service
configuration and test environment details. Do not commit credentials or include
them in issue reports or test output.

Do not report security vulnerabilities or share exploit details in public issues.

## Updating Python Dependencies

Published package dependencies are managed as abstract ranges in
`requirements.in`; do not hash-lock them because this library is installed into
downstream applications. Development, test, documentation, and lock-tooling
environments are managed through human-edited `.in` files under
`requirements/` and generated hash-locked `.txt` files under
`requirements/`. Edit the relevant source file, regenerate the matching
lock file with the exact `pip-compile` command documented below, and include
both files in the pull request. Do not edit generated lock files by hand.

See [internal/specs/python-dependency-hashes.md](internal/specs/python-dependency-hashes.md)
for the exact commands.

## Changelog and Releases

Add user-facing changes under `[Unreleased]` in `CHANGELOG.md`; ordinary PRs
do not change `vbase/_version.py`. Explain why an entry is unnecessary for
infrastructure-only changes.

Follow [RELEASE_MANAGEMENT.md](RELEASE_MANAGEMENT.md) to select a patch, minor,
or major increment in **Prepare release**, approve workflows on its generated
PR when prompted, and review and merge that PR when ready to publish. Merging
a reviewed Release PR automatically tags its merged commit and starts
publication. Shared tooling is maintained in `validityBase/vbase-github-actions`.
