from typing import Literal

import pandas as pd

from humblepy.transform._hashlib import _get_hashlib_hash_function


def _with_pandas_hash_value_column(
    df: pd.DataFrame,
    columns_to_hash: list[str],
    hash_algorithm: Literal["sha256", "sha512", "md5"],
    sort_columns: bool,
    concat_with: str,
    replace_null_with: str,
    uppercase: bool,
    strip_whitespace: bool,
    hash_value_column_name: str,
) -> pd.DataFrame:
    """A deep copy of the pandas DataFrame passed in, with an additional column containing hashed values.

    Args:
        df (pd.DataFrame): pandas DataFrame to generate a hash value column from.
        columns_to_hash (list): List of DataFrame columns to hash.
        hash_algorithm (Literal["sha256", "sha512", "md5"]):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5").
        sort_columns (bool): If True, the DataFrame columns in `columns_to_hash` will be sorted alphabetically prior to hashing.
        concat_with (str): The string value to concatenate column value strings together with, before hashing. Using the default `concat_with` value, a row with values ("apple", "banana") will be concatenated as "apple||banana".
        replace_null_with (str): The string value to replace nulls with, before hashing. Using the default `replace_null_with` and `concat_with` values, a row with values (null, null) will be concatenated as "^^||^^".
        uppercase (bool): If True, values will be converted to uppercase before hashing.
        strip_whitespace (bool): If True, values will have leading and trailing whitespaces removed before hashing.
        hash_value_column_name (str): The name of the hash value column to be added.

        # noqa: DAR103 sort_columns
        # noqa: DAR103 hash_algorithm
        # noqa: DAR103 concat_with
        # noqa: DAR103 replace_null_with
        # noqa: DAR103 hash_value_column_name
        # noqa: DAR103 uppercase
        # noqa: DAR103 strip_whitespace

    Returns:
        pd.DataFrame: A deep copy of the pandas DataFrame passed in, with an additional column containing hashed values.
    """

    # Get hashlib function
    hash_function = _get_hashlib_hash_function(hash_algorithm)
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

    # Create hash value column
    _df[hash_value_column_name] = _sort_columns(
        _df[columns_to_hash].astype("string").fillna(replace_null_with)
    ).apply(
        lambda row: hash_function(
            concat_with.join(map(normalise, row)).encode("utf-8")  # type: ignore
        ).hexdigest(),
        axis=1,
    )

    return _df
