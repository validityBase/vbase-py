"""Check Markdown link destinations in the PyPI README."""

from pathlib import Path
from urllib.parse import urlsplit

from markdown_it import MarkdownIt


def invalid_destinations(markdown: str) -> list[tuple[int, str]]:
    """Return the source block line and URL for invalid Markdown destinations."""
    invalid = []
    for block in MarkdownIt("commonmark").parse(markdown):
        if block.map is None:
            continue
        for token in block.children or ():
            attribute = {"link_open": "href", "image": "src"}.get(token.type)
            if attribute is None:
                continue
            destination = token.attrGet(attribute) or ""
            try:
                url = urlsplit(destination)
                if url.scheme in ("http", "https") and url.hostname:
                    continue
            except ValueError:
                pass
            invalid.append((block.map[0] + 1, destination))
    return invalid


def main() -> int:
    """Report Markdown links and images that lack an absolute HTTP(S) URL."""
    readme = Path("README.md").read_text(encoding="utf-8")
    invalid = invalid_destinations(readme)
    for number, destination in invalid:
        print(f"README.md:{number}: use an absolute HTTP(S) URL for {destination!r}")
    return int(bool(invalid))


if __name__ == "__main__":
    raise SystemExit(main())
