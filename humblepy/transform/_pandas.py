from typing import Literal

import pandas as pd

from humblepy.transform._hashlib import _get_hashlib_hash_function


def _with_pandas_hash_column(
    df: pd.DataFrame,
    columns_to_hash: list[str],
    hash_algorithm: Literal["sha256", "sha512", "md5"],
    is_key: bool,
    sort_columns: bool,
    concat_with: str,
    replace_null_with: str,
    uppercase: bool,
    strip_whitespace: bool,
    hash_column_name: str,
) -> pd.DataFrame:
    """A deep copy of the pandas DataFrame passed in, with an additional column containing hashed values.

    Args:
        df (pd.DataFrame): pandas DataFrame to generate a hash value column from.
        columns_to_hash (list): List of DataFrame columns to hash.
        hash_algorithm (Literal["sha256", "sha512", "md5"]):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5").
        is_key (bool): If False, the hash values column will contain no nulls. If True, the hash value column will be null for any row where all of the values in the `columns_to_hash` columns are null.
        sort_columns (bool): If True, the DataFrame columns in `columns_to_hash` will be sorted alphabetically prior to hashing.
        concat_with (str): The string value to concatenate column value strings together with, before hashing. Using the default `concat_with` value, a row with values ("apple", "banana") will be concatenated as "apple||banana".
        replace_null_with (str): The string value to replace nulls with, before hashing. Using the default `replace_null_with` and `concat_with` values, a row with values (null, null) will be concatenated as "^^||^^".
        uppercase (bool): If True, values will be converted to uppercase before hashing.
        strip_whitespace (bool): If True, values will have leading and trailing whitespaces removed before hashing.
        hash_column_name (str): The name of the hash value column to be added.

    Returns:
        pd.DataFrame: A deep copy of the pandas DataFrame passed in, with an additional column containing hashed values.
    """

    # Get hashlib function
    hash_function = _get_hashlib_hash_function(hash_algorithm)
    # Get the value that will be hashed if all the values in the `columns_to_hash` columns are null, and `is_key` is False.
    # If there are three columns to hash, the string will be "^^||^^||^^" if default values for `concat_with` and `replace_null_with` are used.
    null_string = concat_with.join(replace_null_with for column in columns_to_hash)
    # Get the hash value for the null_string
    null_hash_value = hash_function(null_string.encode()).hexdigest()
    # Create deep copy of input DataFrame
    _df = df.copy()
    # If `sort_columns` is True, sort the columns using for hashing alphabetically
    _sort_columns = lambda x: x.sort_index(axis=1) if sort_columns else x
    # Remove leading and trailing whitespaces
    strip = lambda x: x.strip() if strip_whitespace else x
    # Lambda function to convert string to uppercase if `uppercase` is True
    convert_case = lambda x: x.upper() if uppercase else x
    # Lambda function to combine the previous two lambda functions for readability
    normalise = lambda x: convert_case(strip(x))
    # If `is_key` is True and the hashed value == `null_hash_value`, replace its value with None
    if_key_allow_null = lambda x: None if is_key and x == null_hash_value else x
    # Create hash value column
    _df[hash_column_name] = _sort_columns(
        _df[columns_to_hash].astype("string").fillna(replace_null_with)
    ).apply(
        lambda row: if_key_allow_null(
            hash_function(
                concat_with.join(map(normalise, row)).encode("utf-8")
            ).hexdigest()
        ),
        axis=1,
    )

    return _df
