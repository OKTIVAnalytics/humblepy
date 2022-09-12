import re

import pytest

from humblepy._utilities import _import_optional_dependency, _is_installed


def test_import_optional_depdendency_missing_raise():
    """Tests that the _import_optional_dependency() function raises an ImportError when the module is not found."""
    mock_module_name = "^^"
    with pytest.raises(
        ImportError,
        match=re.escape(
            f"Missing optional dependency. Use pip, conda, or poetry to install {mock_module_name}."
        ),
    ):
        _import_optional_dependency(mock_module_name, errors="raise")


def test_import_optional_depdendency_missing_ignore():
    """Tests that the _import_optional_dependency() function doesn't raise an ImportError when the module is not found and `errors` == "ignore"."""
    mock_module_name = "^^"

    assert _import_optional_dependency(mock_module_name, errors="ignore") is None


def test_is_installed():
    """Tests that the _is_installed() returns False when the module is not found."""
    mock_module_name = "^^"
    assert _is_installed(mock_module_name) is False
