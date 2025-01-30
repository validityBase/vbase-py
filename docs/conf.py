# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "vbase-py"
# Drop the trailing period since Sphinx adds it.
copyright = "2023-2024, PIT Labs, Inc."
author = "PIT Labs, Inc."

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_markdown_builder",
]

# Add Markdown as a supported source format.
source_suffix = [".rst", ".md"]

# Configure Markdown output.
markdown_builder_options = {
    # Set the output folder for Markdown files.
    "output": "docs/_build/markdown",
}

# Tell the myst_parser to generate labels for heading anchors
# for h1 and h2 level headings
# (corresponding to #, ## in markdown).
myst_heading_anchors = 2