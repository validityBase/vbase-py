# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

from sphinx_markdown_builder.translator import (
    MarkdownTranslator as MarkdownTranslatorBase,
)
from sphinx_markdown_builder.translator import (
    TitleContext,
    pushing_context,
)

sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "vbase-py"
copyright = "2023-2026, PIT Labs Inc."
author = "PIT Labs Inc."

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_markdown_builder",
]


class CustomMarkdownTranslator(MarkdownTranslatorBase):
    """Override desc headings for markdown output."""

    def visit_desc_annotation(self, node):
        # Suppress italic wrapping around "class", type annotations, etc.
        pass

    def depart_desc_annotation(self, node):
        pass

    @pushing_context
    def visit_desc_signature(self, node):
        if self.config.markdown_anchor_signatures:
            for anchor in node.get("ids", []):
                self._add_anchor(anchor)

        h_level = 3 if node.get("class", None) else 2
        self._push_context(TitleContext(h_level))


def setup(app):
    app.set_translator("markdown", CustomMarkdownTranslator)
