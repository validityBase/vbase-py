# Python package releases

## Repository configuration

The repository-specific project, version, branch, tag, workflow, and
environment settings are in the
[Release management settings](../../RELEASE_MANAGEMENT.md#repository-settings).

The three local workflow files declare triggers and call shared workflows in
`validityBase/vbase-github-actions`. Generic scripts and tests live there;
this repository contains no copies. Each reusable workflow and its
`release-tools-ref` must use the same pinned shared commit. Optional input
settings, including commit author defaults, are documented in the shared
[release contract](https://github.com/validityBase/vbase-github-actions/blob/main/.github/actions/python-release/README.md).

The upload job runs in this repository's `publish-pypi.yml`, because PyPI's
trusted publisher for `vbase` is registered for this repository, that workflow
file, and the `pypi` environment. It downloads the validated artifact and
invokes the pinned PyPA publisher directly. GitHub Release creation depends
on successful upload.

For manual recovery, dispatch `publish-pypi.yml` from `main` with an existing,
unpublished `release-tag`. The same tag is forwarded to build and GitHub Release
creation; manual and tag-triggered publication share a lock for that full tag ref.

The shared contract defines release behavior and validation; it is not copied
into this repository. Package-specific settings are in the release management
table linked above.

Contributor guidance: [CONTRIBUTING.md](../../CONTRIBUTING.md).
Repository release settings and runbook link: [RELEASE_MANAGEMENT.md](../../RELEASE_MANAGEMENT.md).
