"""Check that pre-commit formatter revisions match the development lock."""

import re
import sys
from pathlib import Path

import yaml

FORMATTER_REPOS = {
    "black": "https://github.com/psf/black",
    "isort": "https://github.com/pycqa/isort",
}


def main() -> int:
    """Report missing or mismatched formatter pins."""
    config = yaml.safe_load(Path(".pre-commit-config.yaml").read_text(encoding="utf-8"))
    lock = Path("requirements/dev.txt").read_text(encoding="utf-8")
    revisions = {
        (repo["repo"].casefold(), hook["id"]): str(repo.get("rev", ""))
        for repo in config["repos"]
        for hook in repo["hooks"]
    }

    errors = []
    for package, repository in FORMATTER_REPOS.items():
        pin = re.search(rf"^{package}==([^\s]+)", lock, re.MULTILINE)
        expected = pin[1] if pin else None
        revision = revisions.get((repository, package))
        if not expected or not revision or revision.removeprefix("v") != expected:
            errors.append(
                f"{package}: hook revision {revision or 'missing'}, "
                f"lock version {expected or 'missing'}"
            )
    for error in errors:
        print(error, file=sys.stderr)
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
