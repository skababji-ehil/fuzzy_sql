# -- Path setup --------------------------------------------------------------
import os
import sys

sys.path.insert(0,os.path.abspath("./src"))
#sys.path.append('/home/samer/projects/fuzzy_sql/src') #This will enable reading the modules


# -- Project information -----------------------------------------------------
project = 'Fuzzy SQL'
copyright = 'Electronic Health Information Laboratory (EHIL) - 2023'
author = 'Samer El Kababji @ EHIL'
version = 'v2.0-beta'
release = '2.0-beta' 



# -- General configuration ---------------------------------------------------
master_doc = 'index'

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinxcontrib.bibtex',
    'sphinx.ext.napoleon',
    'sphinx_rtd_theme',
]


templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']




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






# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
htmlhelp_basename = "fuzzysqlpydoc"

# html_theme_options = {
#     "nosidebar": True,
# }

# Setting latex env by SMK

_PREAMBLE = r"""
\usepackage{amsmath}
\usepackage{amssymb}
\usepackage{amsxtra}
"""



latex_elements = {
'preamble': _PREAMBLE,
}
