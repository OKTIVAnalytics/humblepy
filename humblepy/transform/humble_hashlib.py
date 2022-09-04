from hashlib import md5, sha256, sha512


def _get_hashlib_hash_function_dict():
    """Returns dictionary of hashlib functions.

    Returns:
        dict: Dictionary of hashlib functions.
    """
    return {"sha256": sha256, "sha512": sha512, "md5": md5}
