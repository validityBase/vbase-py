# Release management

Use the [shared release runbook](https://github.com/validityBase/vbase-github-actions/blob/main/RELEASE_MANAGEMENT.md)
for CI approval, Release PR review, publication, and publication checks.
The shared repository is `vbase-github-actions`.
It contains the release tools and their tests.

## Repository settings

| Setting | Value |
| --- | --- |
| Repository | `validityBase/vbase-py` |
| PyPI project | [`vbase`](https://pypi.org/project/vbase/) |
| Import name | `vbase` |
| Version file | `vbase/_version.py` |
| Changelog | `CHANGELOG.md` |
| Default branch | `main` |
| Release branch / tag | `release/vX.Y.Z` / `vX.Y.Z` |
| Publication workflow / environment | `publish-pypi.yml` / `pypi` |

## Preparation conditions

`CHANGELOG.md` must contain user-facing entries under `[Unreleased]`.
The **Prepare release** workflow stops if this section has no entries.
It does not generate release notes from commit history.

Keep `vbase/_version.py` unchanged in the changelog PR.
The **Prepare release** workflow applies the selected version increment.

Review compatibility changes before you select the increment.
The current changes remove support for Python 3.8 through 3.10.
A major increment is recommended for this compatibility change.

## Start the Prepare release workflow

1. Add user-facing entries under `[Unreleased]` in `CHANGELOG.md`.
2. Merge the changelog PR into `main`.
3. Open this repository's **Actions** page.
4. Select **Prepare release**.
5. Open the **Run workflow** menu.
6. Select the `main` branch.
7. Select `patch`, `minor`, or `major`.
8. Select **Run workflow**.

If preparation stopped because release notes were missing, start a new run
after the changelog PR merges.

**Warning:** Merging the generated Release PR starts automatic publication.
Merge that Release PR only when you are ready to publish.

## Checks and their limits

| Check | What the check establishes | Limit |
| --- | --- | --- |
| Inspect the `[Unreleased]` entries. | The changelog contains release notes. | A reviewer must check their accuracy against the source changes. |
| Run the release tools in a disposable copy. | The tested version and changelog pass local preparation and validation. | This does not test GitHub permissions, PyPI authorization, or uploads. |
| Review the Release PR's CI results. | The checks that ran passed for the tested commit. | Passing checks do not establish that every release path works. |
| Follow the shared runbook's publication checks. | The checks can establish whether the published files, version, release notes, and installed package match the release. | These checks require an actual publication. |

Shared inputs and defaults are in the
[action contract](https://github.com/validityBase/vbase-github-actions/blob/main/.github/actions/python-release/README.md).
