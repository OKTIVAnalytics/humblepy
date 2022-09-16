from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Type, Union

import re

import pandas as pd

from humblepy._utilities import _is_installed
from humblepy.transform._hashlib import _get_hashlib_hash_function
from humblepy.transform._pandas import _with_pandas_hash_column
from humblepy.transform._pyspark import _with_pyspark_hash_column

if TYPE_CHECKING or _is_installed("pyspark.sql"):
    import pyspark.sql as pq


def with_hash_column(
    df: Union[pd.DataFrame, pq.DataFrame],
    columns_to_hash: list[str],
    hash_algorithm: Literal["sha256", "sha512", "md5"] = "sha256",
    sort_columns: Optional[bool] = False,
    is_key: Optional[bool] = False,
    concat_with: Optional[str] = "||",
    replace_null_with: Optional[str] = "^^",
    uppercase: Optional[bool] = True,
    strip_whitespace: Optional[bool] = True,
    hash_column_name: Optional[str] = "hash_value",
) -> Union[pd.DataFrame, pq.DataFrame]:
    """Returns a hash value column for a pandas DataFrame.

    Args:
        df (Union[pd.DataFrame, pq.DataFrame]): pandas or PySpark DataFrame to generate a hash value column from.
        hash_algorithm (Literal["sha256", "sha512", "md5"], optional):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5"). Defaults to "sha256".
        columns_to_hash (list): List of DataFrame columns to hash.
        sort_columns (Optional[bool]): If True, the DataFrame columns in `columns_to_hash` will be sorted alphabetically prior to hashing. Defaults to False.
        is_key (Optional[bool]): If False, the hash values column will contain no nulls. If True, the hash value column will be null for any row where all of the values in the `columns_to_hash` columns are null. Defaults to False.
        concat_with (Optional[str]): The string value to concatenate column value strings together with, before hashing. Using the default `concat_with` value, a row with values ("apple", "banana") will be concatenated as "apple||banana". Defaults to "||".
        replace_null_with (Optional[str]): The string value to replace nulls with, before hashing. Using the default `replace_null_with` and `concat_with` values, a row with values (null, null) will be concatenated as "^^||^^". Defaults to "^^".
        uppercase (Optional[bool]): If True, values will be converted to uppercase before hashing. Defaults to True.
        strip_whitespace (Optional[bool]): If True, values will have leading and trailing whitespaces removed before hashing. Defaults to True.
        hash_column_name (Optional[str]): The name of the hash value column to be added. Defaults to "hash_value".

    Returns:
        Union[pd.DataFrame, pq.DataFrame, None]:
            If a pandas DataFrame is passed in, it will return a deep copy of the DataFrame with an additional column containing hashed values.
            If a PySpark DataFrame is passed in, it will return a new PySpark DataFrame based on the DataFrame passed in, with an additional column containing hashed values.
            If `df` is neither a pandas DataFrame nor a PySpark DataFrame, it will return None.
    """

    assert hash_algorithm in ("sha256", "sha512", "md5")

    # If `df` is a pandas DataFrame, call the `_with_pandas_hash_column()` function.
    if isinstance(df, pd.DataFrame):
        return _with_pandas_hash_column(
            df=df,
            columns_to_hash=columns_to_hash,
            hash_algorithm=hash_algorithm,
            sort_columns=sort_columns,  # type: ignore
            is_key=is_key,  # type: ignore
            concat_with=concat_with,  # type: ignore
            replace_null_with=replace_null_with,  # type: ignore
            uppercase=uppercase,  # type: ignore
            strip_whitespace=strip_whitespace,  # type: ignore
            hash_column_name=hash_column_name,  # type: ignore
        )
    # If `df` is a Pyspark DataFrame, call the `_with_pyspark_hash_column()` function.
    elif _is_installed("pyspark.sql") and isinstance(df, pq.DataFrame):
        return _with_pyspark_hash_column(
            df=df,
            columns_to_hash=columns_to_hash,
            hash_algorithm=hash_algorithm,
            sort_columns=sort_columns,  # type: ignore
            is_key=is_key,  # type: ignore
            concat_with=concat_with,  # type: ignore
            replace_null_with=replace_null_with,  # type: ignore
            uppercase=uppercase,  # type: ignore
            strip_whitespace=strip_whitespace,  # type: ignore
            hash_column_name=hash_column_name,  # type: ignore
        )
    else:
        return None


def get_file_checksum(
    file_path: str, hash_algorithm: Literal["sha256", "sha512", "md5"] = "sha256"
) -> str:
    """Returns a checksum value for a file.

    Args:
        file_path (str): The path of the file to read, including the file name and extension.
        hash_algorithm (Literal["sha256", "sha512", "md5"], optional):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5"). Defaults to "sha256".

    Returns:
        (str): String representing the hash value.
    """
    assert hash_algorithm in ("sha256", "sha512", "md5")
    # Get hashlib function
    hash_function = _get_hashlib_hash_function(hash_algorithm)

    # Read file as bytes
    with open(file_path, "rb") as f:
        return hash_function(f.read()).hexdigest()


def get_string_normalised(
    string: str,
    uppercase: Optional[bool] = True,
    lowercase: Optional[bool] = False,
    strip_whitespace: Optional[bool] = True,
    remove_punctuation: Optional[bool] = True,
    remove_numbers: Optional[bool] = False,
    remove_whitespace: Optional[bool] = False,
    replace_whitespace_with: Union[str, None] = None,
) -> str:
    """Returns a normalised version of the string passed in.
    The order the transformations are applied in is:
        1. Convert case (if `uppercase` is True or `lowercase` is True)
        2. Strip leading and trailing whitespaces (if `strip_whitespace` is True)
        3. Remove punctuation (if `remove_punctuation` is True)
        4. Remove numbers (if `remove_numbers` is True)
        5. Remove whitespaces (if `remove_whitespace` is true)
        6. Replace whitespaces with the value of `replace_whitespace_with` (if `replace_whitespace_with` is a string)

    Args:
        string (str): The string to be normalised. Note: strings are immutable in Python; this function returns a new string.
        uppercase (bool, optional): If True and `lowercase` is False (its default value), the string will be converted to uppercase. Defaults to True.
        lowercase (bool, optional): If True, the string will be converted to lowercase. Defaults to False.
        strip_whitespace (bool, optional): If True, leading and trailing whitespaces will be removed from the string. Defaults to True.
        remove_punctuation (bool, optional): If True, every character except alphanumeric characters, underscores, and whitespaces will be removed. Defaults to True.
        remove_numbers (bool, optional): If True, numeric characters will be removed from the string. Defaults to False.
        remove_whitespace (bool, optional): If True, all whitespaces (including spaces, tabs, and linebreaks) will be removed from the string. Defaults to False.
        replace_whitespace_with (Union[str, None], optional): If `replace_whitespace_with` is a string, whitespaces (including spaces, tabs, and linebreaks) in the string will be replaced with this value. Defaults to None.

    Raises:
        TypeError: TypeError if `string` not instance of <class 'str'>.

    Returns:
        str: A normalised version of the string passed in.
    """
    if not isinstance(string, str):
        raise TypeError(f"`string` must be {str}. Found {type(string)}.")

    _convert_case = (
        lambda x: x.upper()
        if uppercase and not lowercase
        else x.lower()
        if lowercase
        else x
    )
    _strip_whitespace = lambda x: x.strip() if strip_whitespace else x
    _remove_punctuation = (
        lambda x: re.sub(r"[^\w\s]", "", x) if remove_punctuation else x
    )
    _remove_numbers = lambda x: re.sub(r"\d+", "", x) if remove_numbers else x
    _replace_whitespace_with = (
        lambda x: re.sub(r"\s+", replace_whitespace_with, x)
        if isinstance(replace_whitespace_with, str)
        else x
    )
    _remove_whitespace = lambda x: re.sub(r"\s+", "", x) if remove_whitespace else x
    _normalise = lambda x: _remove_whitespace(
        _replace_whitespace_with(
            _remove_numbers(_remove_punctuation(_strip_whitespace(_convert_case(x))))
        )
    )

    return str(_normalise(string))
