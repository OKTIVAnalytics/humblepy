from types import ModuleType
from typing import Literal, Optional, Union

import importlib


def _import_optional_dependency(
    module: str, errors: Literal["raise", "ignore"] = "raise"
) -> Optional[Union[ModuleType, None]]:
    """Import an optional dependency.

    Args:
        module (str): Name of the module to be imported.
        errors (Literal["raise", "ignore"], optional): Whether to raise or ignore the ImportError if the module is not found. Defaults to "raise".

    Raises:
        ImportError: If the module is not found and `errors` == "raise'.

    Returns:
        Optional[Union[ModuleType, None]]:
            The imported module if it's found.
            None if the module is not found and `errors` != "raise".
    """

    try:
        return importlib.import_module(module)
    except ImportError:
        if errors == "raise":
            raise ImportError(
                f"Missing optional dependency. Use pip, conda, or poetry to install {module}."
            )

        else:
            return None


def _is_installed(module: str) -> bool:
    """Checks whether an optional module is installed.

    Args:
        module (str): Name of the module to be imported.

    Returns:
        bool: True if the module is installed, else False.
    """
    return (
        True
        if _import_optional_dependency(module, errors="ignore") is not None
        else False
    )
