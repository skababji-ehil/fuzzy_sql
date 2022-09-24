# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Fuzzy SQL'
copyright = 'EHIL 2022'
author = 'Samer Kababji @ EHIL'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
import sys
sys.path.append('/home/samer/projects/fuzzy_sql/src') #This will enable reading the modules


extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']



# Setting latex env by SMK

_PREAMBLE = r"""
\usepackage{amsmath}
\usepackage{amssymb}
\usepackage{amsxtra}
"""



latex_elements = {
'preamble': _PREAMBLE,
}
