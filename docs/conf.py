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

# Use theme suitable for import into external docs.
html_theme = "sphinx_rtd_theme"

# Hide the Sphinx footer text.
# These settings are used by some themes and may give warning with unsupported themes.
html_show_sphinx = False
html_show_sourcelink = False
html_theme_options = {
}
