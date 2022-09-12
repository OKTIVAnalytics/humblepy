import pandas as pd
import pyspark.sql as pq
import pytest

from humblepy.transform.functions import with_hash_value_column


def test_with_hash_value_column_pandas(numeric_with_nulls_pandas_df_fixture):
    """Tests the with_hash_value_column() to make sure it returns a pandas DataFrame if a pandas DataFrame is passed in.

    Args:
        numeric_with_nulls_pandas_df_fixture (callable): pytest fixture for a pandas DataFrame containing mostly numeric values, with some missing values (NaNs).
    """
    assert isinstance(
        with_hash_value_column(
            numeric_with_nulls_pandas_df_fixture,
            columns_to_hash=["a", "b"],
        ),
        pd.DataFrame,
    )


def test_with_hash_value_column_pyspark(numeric_with_nulls_pyspark_df_fixture):
    """Tests the with_hash_value_column() to make sure it returns a PySpark DataFrame if a PySpark DataFrame is passed in.

    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing mostly numeric values, with some missing values (NaNs).
    """
    assert isinstance(
        with_hash_value_column(
            numeric_with_nulls_pyspark_df_fixture,
            columns_to_hash=["a", "b"],
        ),
        pq.DataFrame,
    )


def test_with_hash_value_column_none():
    """Tests the with_hash_value_column() to make sure it returns None if passed something other than a pandas or PySpark DataFrame."""
    assert (
        with_hash_value_column(
            (0, 1),
            columns_to_hash=["a", "b"],
        )
        is None
    )
