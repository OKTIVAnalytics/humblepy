from unittest.mock import mock_open, patch

import pandas as pd
import pyspark.sql as pq
import pytest

from humblepy.transform.functions import get_file_checksum, with_hash_column


def test_with_hash_column_pandas(numeric_with_nulls_pandas_df_fixture):
    """Tests the with_hash_column() to make sure it returns a pandas DataFrame if a pandas DataFrame is passed in.

    Args:
        numeric_with_nulls_pandas_df_fixture (callable): pytest fixture for a pandas DataFrame containing mostly numeric values, with some missing values (NaNs).
    """
    assert isinstance(
        with_hash_column(
            numeric_with_nulls_pandas_df_fixture,
            columns_to_hash=["a", "b"],
        ),
        pd.DataFrame,
    )


def test_with_hash_column_pyspark(numeric_with_nulls_pyspark_df_fixture):
    """Tests the with_hash_column() to make sure it returns a PySpark DataFrame if a PySpark DataFrame is passed in.

    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing mostly numeric values, with some missing values (NaNs).
    """
    assert isinstance(
        with_hash_column(
            numeric_with_nulls_pyspark_df_fixture,
            columns_to_hash=["a", "b"],
        ),
        pq.DataFrame,
    )


def test_with_hash_column_none():
    """Tests the with_hash_column() to make sure it returns None if passed something other than a pandas or PySpark DataFrame."""
    assert (
        with_hash_column(
            (0, 1),
            columns_to_hash=["a", "b"],
        )
        is None
    )


@patch("builtins.open", mock_open(read_data="humblepy is tasty".encode()))
def test_get_file_checksum():
    """Tests the get_file_checksum() function with a mocked input file."""
    result = get_file_checksum(file_path="/path/file.txt")
    assert result == "cdfb7eedb2e18788f0ce8bdf2914a06debde937defd84b329fb496a50fd8928c"
