"""Regression tests for the PyPI README link check."""

import unittest

from check_readme_links import invalid_destinations


class ReadmeLinksTest(unittest.TestCase):
    """Exercise Markdown link syntax used by contributors."""

    def test_relative_reference_link(self):
        """Reject a reference-style link to a repository file."""
        markdown = "[Guide][license]\n\n[license]: LICENSE.txt\n"
        self.assertEqual(invalid_destinations(markdown), ["LICENSE.txt"])

    def test_angle_wrapped_url(self):
        """Accept an absolute URL wrapped in angle brackets."""
        self.assertEqual(invalid_destinations("[Site](<https://example.com>)"), [])

    def test_links_in_code_are_ignored(self):
        """Avoid treating Markdown examples as README links."""
        self.assertEqual(invalid_destinations("```md\n[Guide](guide.md)\n```"), [])

    def test_relative_raw_html_destinations(self):
        """Reject relative targets from raw HTML anchors and images."""
        markdown = (
            '<a href="LICENSE.txt">License</a>\n\n<IMG\n SRC="demo&amp;notes.png" />'
        )
        self.assertEqual(
            invalid_destinations(markdown), ["LICENSE.txt", "demo&notes.png"]
        )

    def test_absolute_raw_html_destinations(self):
        """Accept absolute HTML targets and ignore named anchors and comments."""
        markdown = (
            '<a id="example"></a><a href="https://example.com">Example</a>\n\n'
            '<img src="https://example.com/demo.png">\n\n'
            '<!-- <a href="example.md">Ignored</a> -->'
        )
        self.assertEqual(invalid_destinations(markdown), [])
