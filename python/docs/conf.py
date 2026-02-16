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
    "member-order": "groupwise",
    "inherited-members": False,
    "private-members": False,
    "exclude-members": (
        # Pydantic v2 model methods
        "model_config,model_fields,model_dump,model_dump_json,model_validate,"
        "model_validate_json,model_validate_strings,model_copy,model_post_init,"
        "model_rebuild,model_json_schema,model_fields_set,"
        # Pydantic v1 methods
        "dict,json,parse_obj,parse_raw,validate,copy,construct,from_orm,schema,schema_json,"
        # Pydantic BaseModel dunder methods and attributes
        "__fields__,__validators__,__config__,__fields_set__,"
        "__pydantic_decorators__,__pydantic_complete__,__pydantic_custom_init__,"
        "__pydantic_fields_set__,__pydantic_generic_metadata__,__pydantic_parent_namespace__,"
        "__pydantic_post_init__,__pydantic_private__,__pydantic_self_init__,"
        "__private_attributes__,__signature__,__version__,__repr_args__,__repr_name__"
    ),
}

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
    "show_nav_level": 2,
}
