# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Fuzzy SQL User Guide'
copyright = 'Electronit Health Information Laboratory (EHIL) - 2023'
author = 'Samer El Kababji @ EHIL'
version = 'v2.0.2-beta'
release = '2.0.2-beta' 

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
import sys
sys.path.append('/home/samer/projects/fuzzy_sql/src') #This will enable reading the modules


extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinxcontrib.bibtex',
    'sphinx.ext.napoleon',
]

autoclass_content = 'both'

autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}


# autodoc_default_options = {
#     'members': True,
#     'member-order': 'bysource',
#     'special-members': '__init__',
#     'undoc-members': True,
#     'exclude-members': '__weakref__'
# }

# # Napoleon settings
# napoleon_google_docstring = True
# napoleon_numpy_docstring = True
# napoleon_include_init_with_doc = True
# napoleon_include_private_with_doc = True
# napoleon_include_special_with_doc = True
# napoleon_use_admonition_for_examples = True
# napoleon_use_admonition_for_notes = True
# napoleon_use_admonition_for_references = True
# napoleon_use_ivar = True
# napoleon_use_param = True
# napoleon_use_rtype = True
# napoleon_preprocess_types = True
# napoleon_type_aliases = None
# napoleon_attr_annotations = True



bibtex_bibfiles = ['refs.bib']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'classic'
html_static_path = ['_static']

html_theme_options = {
    "nosidebar": True,
}

# Setting latex env by SMK

_PREAMBLE = r"""
\usepackage{amsmath}
\usepackage{amssymb}
\usepackage{amsxtra}
"""



latex_elements = {
'preamble': _PREAMBLE,
}
