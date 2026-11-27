"""Check link and image destinations in the rendered PyPI README."""

import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

from markdown_it import MarkdownIt

# PyPI renders the README as GitHub Flavored Markdown, whose tag filter
# escapes these tags in raw HTML. HTMLParser would read the text after an
# unescaped one as raw text and miss the links in it.
_GFM_FILTERED_TAG = re.compile(
    r"<(?=/?(?:title|textarea|style|xmp|iframe|noembed|noframes|script|plaintext)"
    r"(?:\s|/?>))",
    re.IGNORECASE,
)


class _ReadmeLinks(HTMLParser):
    """Collect invalid destinations from rendered links and images."""

    def __init__(self) -> None:
        super().__init__()
        self.invalid: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attribute = {"a": "href", "img": "src"}.get(tag)
        if attribute is None:
            return
        for name, value in attrs:
            if name != attribute:
                continue
            destination = value or ""
            try:
                url = urlsplit(destination)
                if url.scheme in ("http", "https") and url.hostname:
                    continue
            except ValueError:
                pass
            self.invalid.append(destination)


def invalid_destinations(markdown: str) -> list[str]:
    """Return invalid destinations from Markdown and raw HTML."""
    renderer = MarkdownIt("commonmark")
    # markdown-it renders links to file:, javascript:, and similar URLs as
    # text. Keep them as links so that they are reported.
    renderer.validateLink = lambda url: True
    parser = _ReadmeLinks()
    parser.feed(_GFM_FILTERED_TAG.sub("&lt;", renderer.render(markdown)))
    parser.close()
    return parser.invalid


def main() -> int:
    """Report links and images that lack an absolute HTTP(S) URL."""
    readme = Path("README.md").read_text(encoding="utf-8")
    invalid = invalid_destinations(readme)
    for destination in invalid:
        print(f"README.md: use an absolute HTTP(S) URL for {destination!r}")
    return int(bool(invalid))


if __name__ == "__main__":
    raise SystemExit(main())
