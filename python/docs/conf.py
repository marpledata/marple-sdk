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
    "sphinxcontrib.autodoc_pydantic",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "member-order": "groupwise",
    "inherited-members": False,
    "private-members": False,
}

# Pydantic Settings: Clean up the docs
autodoc_pydantic_model_show_json = False
autodoc_pydantic_model_show_config = False
autodoc_pydantic_settings_show_json = False

# This hides the 'model_dump', 'model_validate', etc. methods
autodoc_pydantic_model_members = False

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_logo = "_static/logo.png"
html_favicon = "_static/favicon.png"

html_theme_options = {
    "header_links_before_dropdown": 6,
    "navigation_with_keys": True,
    "use_edit_page_button": True,
    "show_nav_level": 2,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/marpledata/marple-sdk",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "GitLab",
            "url": "https://gitlab.com/marpledata/marple-sdk",
            "icon": "fa-brands fa-gitlab",
        },
    ],
}

html_context = {
    "github_url": "https://github.com",
    "github_user": "marpledata",
    "github_repo": "marple-sdk",
    "github_version": "main",
    "doc_path": "python/docs",
    "gitlab_url": "https://gitlab.com",
    "gitlab_user": "marpledata",
    "gitlab_repo": "marple-sdk",
    "gitlab_version": "main",
}


def skip_pydantic_internals(app, what, name, obj, skip, options):
    """
    Triggers on every member. If it looks like a Pydantic internal, skip it.
    """
    # List of exact names to skip (Pydantic V1/V2 compat methods)
    pydantic_internals = {
        "model_copy",
        "model_dump",
        "model_dump_json",
        "model_json_schema",
        "model_parametrized_name",
        "model_post_init",
        "model_rebuild",
        "model_validate",
        "model_validate_json",
        "schema",
        "schema_json",
        "update_forward_refs",
        "model_validate_strings",
        "parse_file",
        "parse_obj",
        "parse_raw",
        "from_orm",
        "validate",
        "construct",
        "copy",
        "json",
        "dict",
    }

    # If the name is in our blacklist, SKIP it (return True)
    if name in pydantic_internals:
        return True

    # Optional: Skip anything starting with "model_" if you are bold
    if name.startswith("model_"):
        return True

    if name.startswith("_"):
        return True

    return skip


def setup(app):
    # Register the function above to run during the build
    app.connect("autodoc-skip-member", skip_pydantic_internals)
