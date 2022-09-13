from __future__ import annotations

from typing import Literal, Union

import hashlib
from collections.abc import Callable


def _get_hashlib_hash_function(
    hash_algorithm: Literal["sha256", "sha512", "md5"]
) -> Callable[[Union[str, bytes]], hashlib._Hash]:
    """Returns the hashlib function for the hash algorithm name passed in.
    hash_algorithm (Literal["sha256", "sha512", "md5"]):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5").
    Returns:
        Callable[[Union[str, bytes]], hashlib._Hash]: Hashlib function.
    """
    hash_function_dict = {
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
        "md5": hashlib.md5,
    }

    return hash_function_dict[hash_algorithm]  # type: ignore
