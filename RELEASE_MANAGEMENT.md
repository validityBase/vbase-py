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
| Version attribute | `__version__` |
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

Review compatibility changes in `[Unreleased]` before selecting the increment.
Use a major increment when those changes remove supported Python versions
or otherwise break backward compatibility.

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

## Test the release tools in a disposable copy

After `[Unreleased]` has user-facing entries, run the same release tool pinned
by `prepare-release.yml` in a temporary clone. Use Python 3.11 with `build`
installed and choose the increment you plan to use in Prepare release. The
commands clone `main` from GitHub, the branch that Prepare release uses, so
changes not yet merged into `main` are not tested. They run in a subshell from
any directory, stop at the first failed command, and remove the temporary
clone when they finish:

```bash
(
  set -euo pipefail
  WORK=$(mktemp -d)
  trap 'rm -rf "$WORK"' EXIT
  git clone --quiet https://github.com/validityBase/vbase-py "$WORK/vbase-py"
  REF=$(sed -n 's/^ *release-tools-ref: //p' "$WORK/vbase-py/.github/workflows/prepare-release.yml")
  [ -n "$REF" ] || { echo "release-tools-ref not found" >&2; exit 1; }
  git clone --quiet https://github.com/validityBase/vbase-github-actions "$WORK/tools"
  git -C "$WORK/tools" checkout --quiet "$REF"
  TOOL="$WORK/tools/.github/actions/python-release/scripts/release.py"
  VERSION=$(python3.11 "$TOOL" prepare --root "$WORK/vbase-py" --version-file vbase/_version.py --bump patch --notes-file "$WORK/notes.md")
  python3.11 -m build --outdir "$WORK/dist" "$WORK/vbase-py"
  python3.11 "$TOOL" validate --root "$WORK/vbase-py" --version-file vbase/_version.py --tag "v$VERSION" --dist-dir "$WORK/dist" --package-name vbase
  cat "$WORK/notes.md"
)
```

Replace `patch` with `minor` or `major` when appropriate. `prepare` stops if
`[Unreleased]` has no entries that start with `-`; `validate` checks the built
wheel and sdist names and versions. The prepare, build, and validate sequence
was verified in a disposable Python 3.11 copy with a temporary changelog
entry before being documented.

## After publication

Merge `main` back into `dev` after each release. `dev` is protected, so make a
PR from a branch based on `dev`:

```bash
git fetch origin --tags
git switch -c sync/main-into-dev-vX.Y.Z origin/dev
git merge --no-ff origin/main -m "Merge main into dev after the vX.Y.Z release"
```

If the merge stops with conflicts, resolve them and commit before you go on.

Prepare release adds the `## [X.Y.Z]` heading directly below
`## [Unreleased]`. Git can therefore merge changelog entries that reached
`dev` after it was promoted to `main` into the released section without a
conflict. Set `V` to the released version and check that the merge left the
released section unchanged:

```bash
V=X.Y.Z
git rev-parse -q --verify "refs/tags/v$V" >/dev/null &&
  grep -q "^## \[$V\]" CHANGELOG.md &&
  diff <(git show "v$V:CHANGELOG.md" | sed -n "/^## \[$V\]/,\$p") \
    <(sed -n "/^## \[$V\]/,\$p" CHANGELOG.md) &&
  echo "release notes unchanged"
```

The command prints `release notes unchanged` only when the tag exists,
`CHANGELOG.md` has the version's heading, and the released section matches
the tag. If it prints nothing, check `V`. If `diff` shows added entries, move
them under `## [Unreleased]` and commit. Then push the branch and open the PR:

```bash
git push -u origin sync/main-into-dev-vX.Y.Z
gh pr create --base dev --title "Merge main into dev after the vX.Y.Z release" --body "Brings the vX.Y.Z version and changelog from main into dev."
```

Merge the PR with a **merge commit**, not a squash or rebase, so `dev` contains
`main`'s release commit. Otherwise `dev` can retain the old version and list
released changes under `[Unreleased]`. After the merge, check that `dev`
contains the commit that the release tag points to. Check the tag, not
`origin/main`: if `main` gets new commits while the sync PR is open, an
`origin/main` check fails even though `dev` has the release.

```bash
git fetch origin --tags
git merge-base --is-ancestor vX.Y.Z origin/dev && echo synced
```

## Checks and their limits

| Check | What the check establishes | Limit |
| --- | --- | --- |
| Inspect the `[Unreleased]` entries. | The changelog contains release notes. | A reviewer must check their accuracy against the source changes. |
| Before running **Prepare release**, review docs, docstrings, and comments changed since the previous tag against the code. | Release-facing explanations agree with the implementation. | Automated checks cannot establish every behavioral claim. |
| Run the release tools in a disposable copy. | The tested version and changelog pass local preparation and validation. | This does not test GitHub permissions, PyPI authorization, or uploads. |
| Review the Release PR's CI results. | The checks that ran passed for the tested commit. | Passing checks do not establish that every release path works. |
| Follow the shared runbook's publication checks. | The checks can establish whether the published files, version, release notes, and installed package match the release. | These checks require an actual publication. |

Shared inputs and defaults are in the
[action contract](https://github.com/validityBase/vbase-github-actions/blob/main/.github/actions/python-release/README.md).
