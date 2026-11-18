"""Check inline Markdown link destinations in the PyPI README."""

import re
from pathlib import Path


def main() -> int:
    """Report lines with inline links that lack an absolute HTTP(S) URL."""
    readme = Path("README.md").read_text(encoding="utf-8")
    bad_lines = [
        number
        for number, line in enumerate(readme.splitlines(), 1)
        if re.search(r"\]\((?!https?://)[^)]*\)", line)
    ]
    for number in bad_lines:
        print(f"README.md:{number}: link must use an absolute HTTP(S) URL")
    return int(bool(bad_lines))


if __name__ == "__main__":
    raise SystemExit(main())
