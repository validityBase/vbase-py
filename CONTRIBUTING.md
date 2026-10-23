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
