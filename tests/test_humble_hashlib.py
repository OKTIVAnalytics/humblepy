import pytest
from hashlib import sha256, sha512, md5
from humblepy.transform.humble_hashlib import _get_hashlib_hash_function_dict


def test__get_hashlib_hash_function_dict():
    """Tests the _get_hashlib_hash_function_dict() function."""
    assert _get_hashlib_hash_function_dict() == {
        "sha256": sha256,
        "sha512": sha512,
        "md5": md5,
    }
