import pytest

from humblepy.validate._pyspark import _pyspark_dataframes_are_equal


def test_pyspark_dataframes_are_equal(numeric_with_nulls_pyspark_df_fixture):
    """Tests the _pyspark_dataframes_are_equal() function.

    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a list of dicts containing mostly numeric test data, with some null (NoneType) values, to create a DataFrame with.

    """
    assert (
        _pyspark_dataframes_are_equal(
            left=numeric_with_nulls_pyspark_df_fixture,
            right=numeric_with_nulls_pyspark_df_fixture,
        )
        is True
    )
