# Recursive templating from StackOverflow
# See https://stackoverflow.com/questions/2701998/automatically-document-all-modules-recursively-with-sphinx-autodoc/62613202#62613202

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import importlib
import inspect
import os
import subprocess
import sys
from functools import reduce
from pathlib import Path

ROOT = os.path.abspath("../../src")
sys.path.insert(0, ROOT)


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "UA CMS Framework"
copyright = "2025, Nathan A Nguyen"
author = "Nathan A Nguyen"
release = "0.0.1"
githash = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode("ascii")

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.graphviz",
    "sphinx.ext.intersphinx",
    "sphinx.ext.linkcode",
    "sphinx.ext.napoleon",
]
templates_path = ["_templates"]
exclude_patterns = []


## CUSTOM

autosummary_generate = True
# autosummary_imported_members = True
# autosummary_ignore_module_all = True

napoleon_preprocess_types = True
napoleon_type_aliases = {
    "awkward.Array": ":class:`awkward.Array <ak.Array>`",
    "awkward.Record": ":class:`awkward.Record <ak.Record>`",
    "awkward.highlevel.Array": ":class:`awkward.Array <ak.Array>`",
    "awkward.highlevel.Record": ":class:`awkward.Record <ak.Record>`",
    "dask_awkward.Array": ":class:`dask_awkward.Array <dask_awkward.Array>`",
    "dask_awkward.Record": ":class:`dask_awkward.Record <dask_awkward.Record>`",
    "dask_awkward.Scalar": ":class:`dask_awkward.Scalar <dask_awkward.Scalar>`",
}


def linkcode_resolve(domain, info: dict):
    if domain != "py":
        return None
    mod = importlib.import_module(info["module"])
    try:
        obj = reduce(getattr, [mod] + info["fullname"].split("."))
    except AttributeError:
        return None
    try:
        path = inspect.getsourcefile(obj)
        if path is None:
            return None
        relpath = Path(path).relative_to(ROOT)
        _, lineno = inspect.getsourcelines(obj)
    except TypeError:
        # skip property or other type that inspect doesn't like
        return None
    except ValueError:
        return None
    url = f"http://github.com/cmsua/4tops/blob/{githash}/{relpath}#L{lineno}"
    return url


intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "awkward": ("https://awkward-array.org/doc/main/", None),
    "dask-awkward": ("https://dask-awkward.readthedocs.io/en/stable/", None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_baseurl = "/afw/"

html_logo = "logo.png"
html_favicon = "logo.png"
