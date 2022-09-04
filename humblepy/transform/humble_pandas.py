import pandas as pd
from humblepy.transform.humble_hashlib import _get_hashlib_hash_function_dict


def _get_pandas_hash_key_column(
    df: pd.DataFrame,
    columns_to_hash: list,
    hash_algorithm: str = "sha256",
    concat_with: str = "||",
    replace_null_with: str = "^^",
) -> pd.DataFrame:
    """Returns a hash key column for a pandas DataFrame.

    Args:
        df (pd.DataFrame): pandas DataFrame to generate a hash key column from.
        columns_to_hash (list): List of DataFrame columns to hash.
        hash_algorithm (str, optional):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5"). Defaults to "sha256".
        concat_with (str, optional): The string value to concatenate column value strings together with, before hashing. Using the default `concat_with` value, a row with values ("apple", "banana") will be concatenated as "apple||banana". Defaults to "||".
        replace_null_with (str, optional): The string value to replace nulls with, before hashing. Using the default `replace_null_with` and `concat_with` values, a row with values (null, null) will be concatenated as "^^||^^".. Defaults to "^^".

    Returns:
        pd.DataFrame: pandas DataFrame with single column called 'hash_key' containing hashed values.
    """

    # Get dictionary of hashlib functions
    hash_function_dict = _get_hashlib_hash_function_dict()

    # Create hash key column by filtering to the specific columns,
    # converting row to single concatenated string, encoding, and hashing

    return (
        df[columns_to_hash]
        .fillna(replace_null_with)
        .apply(
            lambda row: hash_function_dict[hash_algorithm](
                concat_with.join(map(str, row)).encode("utf-8")
            ).hexdigest(),
            axis=1,
        )
        .to_frame("hash_key")
    )
