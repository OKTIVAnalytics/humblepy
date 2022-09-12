import re

import pytest

from humblepy.validate.functions import dataframes_are_equal


def test_dataframes_are_equal_pandas(numeric_with_nulls_pandas_df_fixture):
    """Tests the dataframes_are_equal() function for comparing two pandas DataFrames.

    Args:
        numeric_with_nulls_pandas_df_fixture (callable): pytest fixture for a pandas DataFrame containing mostly numeric test data, with some null (NoneType) values.

    """
    assert (
        dataframes_are_equal(
            left=numeric_with_nulls_pandas_df_fixture,
            right=numeric_with_nulls_pandas_df_fixture,
        )
        is True
    )


def test_dataframes_are_equal_pyspark(numeric_with_nulls_pyspark_df_fixture):
    """Tests the dataframes_are_equal() function for comparing two PySpark DataFrames.

    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing mostly numeric test data, with some null (NoneType) values.

    """
    assert (
        dataframes_are_equal(
            left=numeric_with_nulls_pyspark_df_fixture,
            right=numeric_with_nulls_pyspark_df_fixture,
        )
        is True
    )


def test_dataframes_are_equal_raise():
    """Tests the dataframes_are_equal() to make sure it raises a TypeError if passed objects of different types.
    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing mostly numeric test data, with some null (NoneType) values.

    """
    left = (0, 1)
    right = [0, 1]
    with pytest.raises(
        TypeError,
        match=re.escape(
            f"Cannot compare DataFrames of different types. `left` is type {type(left)}; `right` is type {type(right)}."
        ),
    ):
        dataframes_are_equal(
            left=left,
            right=right,
        )
