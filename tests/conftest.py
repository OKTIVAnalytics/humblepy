import logging
from unittest import mock

import pandas as pd
import pyspark.sql as pq
import pytest


@pytest.fixture(scope="session")
def spark_session_fixture(request):
    """Fixture for setting up and tearing down a Spark session.
    Args:
        request: pytest.FixtureRequest object.
    Returns:
        pyspark.sql.session.SparkSession: PySpark SQL `SparkSession` object.
    """
    spark_session = pq.SparkSession.builder.enableHiveSupport().getOrCreate()
    request.addfinalizer(lambda: spark_session.stop())
    # Turn down Spark logging for the test session
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)
    return spark_session


@pytest.fixture
def numeric_test_data_fixture():
    """Returns a list of dicts containing numeric test data, to create a DataFrame with.

    Returns:
        list[dict]: A list of dicts with column names as keys and integers as values.
    """
    return [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]


@pytest.fixture
def numeric_pandas_df_fixture(numeric_test_data_fixture):
    """Returns a pandas DataFrame containing numeric data.

    Args:
        numeric_test_data_fixture (callable): pytest fixture for a list of dicts with column names as keys and integers as values.

    Returns:
        pd.DataFrame: pandas DataFrame constructed from `numeric_test_data_fixture`
    """
    return pd.DataFrame(numeric_test_data_fixture)


@pytest.fixture
def numeric_pyspark_df_fixture(spark_session_fixture, numeric_test_data_fixture):
    """Returns a PySpark DataFrame containing numeric data.

    Args:
        spark_session_fixture (callable): pytest fixture for setting up and tearing down a Spark session.
        numeric_test_data_fixture (callable): pytest fixture for a list of dicts with column names as keys and integers as values.

    Returns:
        pq.DataFrame: PySpark DataFrame constructed from `numeric_test_data_fixture`
    """

    return spark_session_fixture.createDataFrame(numeric_test_data_fixture)


@pytest.fixture
def numeric_test_data_with_nulls_fixture():
    """Returns a list of dicts containing mostly numeric test data, with some null (NoneType) values, to create a DataFrame with.

    Returns:
        list[dict]: A list of dicts with column names as keys, and integers and NoneTypes as values.
    """
    return [
        {"a": None, "b": 2, "c": None},
        {"a": 10, "b": None, "c": 30},
    ]


@pytest.fixture
def numeric_with_nulls_pandas_df_fixture(numeric_test_data_with_nulls_fixture):
    """Returns a pandas DataFrame containing mostly numeric data, with some nulls (NaNs).

    Args:
        numeric_test_data_with_nulls_fixture (callable): pytest fixture for a list of dicts with column names as keys, and a mix of integer and NoneType values.

    Returns:
        pd.DataFrame: pandas DataFrame constructed from `numeric_test_data_with_nulls_fixture`
    """
    return pd.DataFrame(numeric_test_data_with_nulls_fixture).astype("Int64")


@pytest.fixture
def numeric_with_nulls_pyspark_df_fixture(
    spark_session_fixture, numeric_test_data_with_nulls_fixture
):
    """Returns a PySpark DataFrame containing mostly numeric data, with some nulls (NaNs).

    Args:
        spark_session_fixture (callable): pytest fixture for setting up and tearing down a Spark session.
        numeric_test_data_with_nulls_fixture (callable): pytest fixture for a list of dicts with column names as keys, and a mix of integer and NoneType values.

    Returns:
        pq.DataFrame: PySpark DataFrame constructed from `numeric_test_data_with_nulls_fixture`
    """

    return spark_session_fixture.createDataFrame(numeric_test_data_with_nulls_fixture)


@pytest.fixture
def string_test_data_fixture():
    """Returns a list of dicts containing string test data, to create a DataFrame with.

    Returns:
        list[dict]: A list of dicts with column names as keys and strings as values.
    """
    return [
        {"a": "apple", "b": "banana", "c": "cherry"},
        {"a": "avocado", "b": "blueberry", "c": "clementine"},
    ]


@pytest.fixture
def string_pandas_df_fixture(string_test_data_fixture):
    """Returns a pandas DataFrame containing string data.

    Args:
        string_test_data_fixture (callable): pytest fixture for a list of dicts with column names as keys and strings as values.

    Returns:
        pd.DataFrame: pandas DataFrame constructed from `string_test_data_fixture`
    """
    return pd.DataFrame(string_test_data_fixture)


@pytest.fixture
def string_pyspark_df_fixture(spark_session_fixture, string_test_data_fixture):
    """Returns a PySpark DataFrame containing string data.

    Args:
        spark_session_fixture (callable): pytest fixture for setting up and tearing down a Spark session.
        string_test_data_fixture (callable): pytest fixture for a list of dicts with column names as keys and strings as values.

    Returns:
        pq.DataFrame: PySpark DataFrame constructed from `string_test_data_fixture`
    """

    return spark_session_fixture.createDataFrame(string_test_data_fixture)


@pytest.fixture
def string_test_data_with_nulls_fixture():
    """Returns a list of dicts containing mostly string test data, with some null (NoneType) values, to create a DataFrame with.

    Returns:
        list[dict]: A list of dicts with column names as keys, and strings and NoneTypes as values.
    """
    return [
        {"a": None, "b": "banana", "c": None},
        {"a": "avocado", "b": None, "c": "clementine"},
    ]


@pytest.fixture
def string_with_nulls_pandas_df_fixture(string_test_data_with_nulls_fixture):
    """Returns a pandas DataFrame containing mostly string data, with some nulls (NaNs).

    Args:
        string_test_data_with_nulls_fixture (callable): pytest fixture for a list of dicts with column names as keys, and a mix of string and NoneType values.

    Returns:
        pd.DataFrame: pandas DataFrame constructed from `string_test_data_with_nulls_fixture`
    """
    return pd.DataFrame(string_test_data_with_nulls_fixture)


@pytest.fixture
def string_with_nulls_pyspark_df_fixture(
    spark_session_fixture, string_test_data_with_nulls_fixture
):
    """Returns a PySpark DataFrame containing mostly string data, with some nulls (NaNs).

    Args:
        spark_session_fixture (callable): pytest fixture for setting up and tearing down a Spark session.
        string_test_data_with_nulls_fixture (callable): pytest fixture for a list of dicts with column names as keys, and a mix of string and NoneType values.

    Returns:
        pq.DataFrame: PySpark DataFrame constructed from `string_test_data_with_nulls_fixture`
    """

    return spark_session_fixture.createDataFrame(string_test_data_with_nulls_fixture)
