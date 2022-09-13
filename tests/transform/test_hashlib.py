import hashlib

import pytest

from humblepy.transform._hashlib import _get_hashlib_hash_function


def test_get_hashlib_hash_function_sha256():
    """Tests the _get_hashlib_hash_function() function for the sha256 algorithm."""
    assert _get_hashlib_hash_function("sha256") == hashlib.sha256


def test_get_hashlib_hash_function_sha512():
    """Tests the _get_hashlib_hash_function() function for the sha512 algorithm."""
    assert _get_hashlib_hash_function("sha512") == hashlib.sha512


def test_get_hashlib_hash_function_md5():
    """Tests the _get_hashlib_hash_function() function for the md5 algorithm."""
    assert _get_hashlib_hash_function("md5") == hashlib.md5
