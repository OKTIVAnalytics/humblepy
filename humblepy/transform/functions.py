import pandas as pd
from humblepy.transform.humble_pandas import _get_pandas_hash_key_column


def get_hash_key_column(
    df: pd.DataFrame, columns_to_hash: list, hash_algorithm: str = "sha256"
) -> pd.DataFrame:
    """Returns a hash key column for a pandas DataFrame.

    Args:
        df (pd.DataFrame): pandas DataFrame to generate a hash key column from.
        columns_to_hash (list): List of DataFrame columns to hash.
        hash_algorithm (str, optional):  Name of the hash algorithm to use. Must be one of ("sha256", "sha512", "md5"). Defaults to "sha256".

    Returns:
        pd.DataFrame: pandas DataFrame with single column called 'hash_key' containing hashed values.
    """

    if isinstance(df, pd.DataFrame):
        return _get_pandas_hash_key_column(df, columns_to_hash, hash_algorithm)
