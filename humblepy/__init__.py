# type: ignore[attr-defined]
"""Tasty recipes for analytics engineering."""

import sys
from importlib import metadata as importlib_metadata


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


version: str = get_version()

# https://packaging.python.org/guides/packaging-namespace-packages/#pkgutil-style-namespace-packages
__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore
