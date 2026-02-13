import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "Marple SDK"
author = "Marple"
copyright = "2026, Marple"

try:
    import marple

    release = marple.__version__
except Exception:
    release = "unknown"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "member-order": "bysource",
    "inherited-members": False,
    "exclude-members": (
        "model_config,model_fields,model_dump,model_dump_json,model_validate,"
        "model_validate_json,model_copy,dict,json,parse_obj,parse_raw,validate,"
        "copy,construct,from_orm,schema,schema_json"
    ),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

html_theme_options = {
    "header_links_before_dropdown": 6,
    "navigation_with_keys": True,
    "show_nav_level": 2,
}
