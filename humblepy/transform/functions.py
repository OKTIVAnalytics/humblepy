from typing import TYPE_CHECKING, Literal, Optional, Union

import pandas as pd

from humblepy._utilities import _is_installed
from humblepy.transform._pandas import _with_pandas_hash_value_column
from humblepy.transform._pyspark import _with_pyspark_hash_value_column

if TYPE_CHECKING or _is_installed("pyspark.sql"):
    import pyspark.sql as pq


def with_hash_value_column(
    df: Union[pd.DataFrame, pq.DataFrame],
    columns_to_hash: list[str],
    hash_algorithm: Literal["sha256", "sha512", "md5"] = "sha256",
    sort_columns: Optional[bool] = False,
    concat_with: Optional[str] = "||",
    replace_null_with: Optional[str] = "^^",
    uppercase: Optional[bool] = True,
    strip_whitespace: Optional[bool] = True,
    hash_value_column_name: Optional[str] = "hash_key",
) -> Union[pd.DataFrame, pq.DataFrame]:
    """Returns a hash value column for a pandas DataFrame.

    Args:
        df (Union[pd.DataFrame, pq.DataFrame]): pandas or PySpark DataFrame to generate a hash value column from.
        hash_algorithm (Literal["sha256", "sha512", "md5"], optional):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5"). Defaults to "sha256".
        columns_to_hash (list): List of DataFrame columns to hash.
        sort_columns (Optional[bool]): If True, the DataFrame columns in `columns_to_hash` will be sorted alphabetically prior to hashing. Defaults to False.
        concat_with (Optional[str]): The string value to concatenate column value strings together with, before hashing. Using the default `concat_with` value, a row with values ("apple", "banana") will be concatenated as "apple||banana". Defaults to "||".
        replace_null_with (Optional[str]): The string value to replace nulls with, before hashing. Using the default `replace_null_with` and `concat_with` values, a row with values (null, null) will be concatenated as "^^||^^". Defaults to "^^".
        uppercase (Optional[bool]): If True, values will be converted to uppercase before hashing. Defaults to True.
        strip_whitespace (Optional[bool]): If True, values will have leading and trailing whitespaces removed before hashing. Defaults to True.
        hash_value_column_name (Optional[str]): The name of the hash value column to be added. Defaults to "hash_key".

    Returns:
        Union[pd.DataFrame, pq.DataFrame, None]:
            If a pandas DataFrame is passed in, it will return a deep copy of the DataFrame with an additional column containing hashed values.
            If a PySpark DataFrame is passed in, it will return a new PySpark DataFrame based on the DataFrame passed in, with an additional column containing hashed values.
            If `df` is neither a pandas DataFrame nor a PySpark DataFrame, it will return None.
    """

    assert hash_algorithm in ("sha256", "sha512", "md5")

    # If `df` is a pandas DataFrame, call the `_with_pandas_hash_value_column()` function.
    if isinstance(df, pd.DataFrame):
        return _with_pandas_hash_value_column(
            df=df,
            columns_to_hash=columns_to_hash,
            hash_algorithm=hash_algorithm,
            sort_columns=sort_columns,
            concat_with=concat_with,
            replace_null_with=replace_null_with,
            uppercase=uppercase,
            strip_whitespace=strip_whitespace,
            hash_value_column_name=hash_value_column_name,
        )
    # If `df` is a Pyspark DataFrame, call the `_with_pyspark_hash_value_column()` function.
    elif _is_installed("pyspark.sql") and isinstance(df, pq.DataFrame):
        return _with_pyspark_hash_value_column(
            df=df,
            columns_to_hash=columns_to_hash,
            hash_algorithm=hash_algorithm,
            sort_columns=sort_columns,
            concat_with=concat_with,
            replace_null_with=replace_null_with,
            uppercase=uppercase,
            strip_whitespace=strip_whitespace,
            hash_value_column_name=hash_value_column_name,
        )
    else:
        return None
