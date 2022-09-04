import pytest
import pandas as pd
from tests.fixtures import numeric_test_data_with_nulls_pandas_df
from humblepy.transform.main import get_hash_key_column


def test_get_hash_key_column_pandas(numeric_test_data_with_nulls_pandas_df):
    """Tests the get_hash_key_column() to make sure it returns a pandas DataFrame if a pandas DataFrame is passed in.

    Args:
        numeric_test_data_with_nulls_pandas_df callable: pytest fixture for a pandas DataFrame containing numeric values and null values (NaN).
    """
    assert isinstance(
        get_hash_key_column(
            numeric_test_data_with_nulls_pandas_df,
            columns_to_hash=["a", "b"],
        ),
        pd.DataFrame,
    )
