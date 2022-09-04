import pandas as pd
import pytest

from humblepy.transform.functions import get_hash_key_column


def test_get_hash_key_column_pandas(numeric_with_nulls_pandas_df_fixture):
    """Tests the get_hash_key_column() to make sure it returns a pandas DataFrame if a pandas DataFrame is passed in.

    Args:
        numeric_with_nulls_pandas_df_fixture (callable): pytest fixture for a pandas DataFrame containing mostly numeric values, with some missing values (NaNs).
    """
    assert isinstance(
        get_hash_key_column(
            numeric_with_nulls_pandas_df_fixture,
            columns_to_hash=["a", "b"],
        ),
        pd.DataFrame,
    )
