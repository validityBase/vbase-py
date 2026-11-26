"""Regression tests for links in HTML rendered from reStructuredText."""

import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


class SphinxLinksTest(unittest.TestCase):
    """Verify that Lychee finds broken relative links in Sphinx HTML."""

    def test_missing_page_and_missing_cross_page_anchor(self):
        """Render both broken links and check that each is reported."""
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            output = root / "html"
            (source / "conf.py").write_text(
                'project = "Link check"\n', encoding="utf-8"
            )
            (source / "index.rst").write_text(
                "Index\n=====\n\n"
                ".. toctree::\n\n"
                "   guide\n\n"
                "See `missing guide <missing-guide.html>`_ "
                "and `missing section <guide.html#absent>`_.\n",
                encoding="utf-8",
            )
            (source / "guide.rst").write_text("Guide\n=====\n", encoding="utf-8")
            subprocess.run(
                ["sphinx-build", "-q", "-W", "-b", "html", str(source), str(output)],
                check=True,
            )
            result = subprocess.run(
                [
                    "lychee",
                    "--offline",
                    "--include-fragments",
                    "--no-progress",
                    str(output / "index.html"),
                ],
                check=False,
                capture_output=True,
                encoding="utf-8",
            )
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            for target in ("missing-guide.html", "guide.html#absent"):
                with self.subTest(target=target):
                    self.assertRegex(
                        result.stdout + result.stderr, rf"\[ERROR\].*{target}"
                    )
