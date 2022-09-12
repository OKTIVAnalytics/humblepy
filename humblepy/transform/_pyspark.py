from typing import Literal, Union

from collections.abc import Callable

from humblepy._utilities import _import_optional_dependency

_import_optional_dependency("pyspark")


import pyspark.sql as pq
import pyspark.sql.functions as F


def _get_pyspark_hash_function(
    hash_algorithm: Literal["sha256", "sha512", "md5"],
) -> Callable[[Union[pq.Column, str]], pq.Column]:
    """Returns the PySpark SQL function for the hash algorithm name passed in.
    hash_algorithm (Literal["sha256", "sha512", "md5"]):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5").
    Returns:
        Callable[[Union[pq.Column, str]], pq.Column]: PySpark SQL function.
    """
    hash_function_dict = {
        "sha256": lambda x: F.sha2(x, 256),
        "sha512": lambda x: F.sha2(x, 512),
        "md5": F.md5,
    }

    return hash_function_dict[hash_algorithm]


def _with_pyspark_hash_value_column(
    df: pq.DataFrame,
    columns_to_hash: list[str],
    hash_algorithm: Literal["sha256", "sha512", "md5"],
    sort_columns: bool,
    concat_with: str,
    replace_null_with: str,
    uppercase: bool,
    strip_whitespace: bool,
    hash_value_column_name: str,
) -> pq.DataFrame:
    """A new PySpark DataFrame based on the DataFrame passed in, with an additional column containing hashed values.

    Args:
        df (pq.DataFrame): PySpark DataFrame to generate a hash value column from.
        columns_to_hash (list): List of DataFrame columns to hash.
        hash_algorithm (Literal["sha256", "sha512", "md5"]): Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5").
        sort_columns (bool, optional): If True, the DataFrame columns in `columns_to_hash` will be sorted alphabetically prior to hashing.
        concat_with (str, optional): The string value to concatenate column value strings together with, before hashing. Using the default `concat_with` value, a row with values ("apple", "banana") will be concatenated as "apple||banana".
        replace_null_with (str, optional): The string value to replace nulls with, before hashing. Using the default `replace_null_with` and `concat_with` values, a row with values (null, null) will be concatenated as "^^||^^".
        uppercase (bool, optional): If True, values will be converted to uppercase before hashing.
        strip (bool, optional): If True, values will have leading and trailing whitespaces removed before hashing.
        hash_value_column_name (str, optional): The name of the hash value column to be added.

        # noqa: DAR103 sort_columns
        # noqa: DAR103 hash_algorithm
        # noqa: DAR103 concat_with
        # noqa: DAR103 replace_null_with
        # noqa: DAR103 hash_value_column_name
        # noqa: DAR103 uppercase
        # noqa: DAR103 strip

    Returns:
        pyspark.sql.DataFrame: A new PySpark DataFrame based on the DataFrame passed in, with an additional column containing hashed values.
    """
    # Get PySpark hash function function
    hash_function = _get_pyspark_hash_function(hash_algorithm)
    # PySpark DataFrames are immutable so there's no need to create a deep copy.
    # This line keeps the `_df` variable name consistent with functions that operate on mutable DataFrames (e.g. pandas).
    _df = df
    # If `sort_columns` is True, sort the columns using for hashing alphabetically
    sorted_columns_to_hash = (
        sorted(columns_to_hash) if sort_columns else columns_to_hash
    )
    # Cast as string
    as_string = lambda x: F.col(x).cast("string").alias(x)
    # Remove leading and trailing whitespaces
    strip = lambda x: F.trim(x) if strip_whitespace else x
    # Convert string to uppercase if `uppercase` is True
    convert_case = lambda x: F.upper(x) if uppercase else x
    # Replace nulls with the `replace_null_with` value
    replace_null = lambda x: F.coalesce(x, F.lit(replace_null_with))
    # Lambda function to combine the previous four lambda functions for readability
    normalise = lambda x: replace_null(convert_case(strip(as_string(x))))
    # Concatenate column strings with the `concat_with` value
    concatenate = lambda x: F.concat_ws(concat_with, *x)

    # Create hash value column
    _df = _df.withColumn(
        hash_value_column_name,
        hash_function(concatenate(normalise(x) for x in sorted_columns_to_hash)),
    )

    return _df
