import pytest
import pandas as pd

numeric_test_data_dict = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]

numeric_test_data_with_nulls_dict = [
    {"a": None, "b": 2, "c": None},
    {"a": 10, "b": None, "c": 30},
]

string_test_data_dict = [
    {"a": "apple", "b": "banana", "c": "cherry"},
    {"a": "avocado", "b": "blueberry", "c": "clementine"},
]

string_test_data_with_nulls_dict = [
    {"a": None, "b": "banana", "c": None},
    {"a": "avocado", "b": None, "c": "clementine"},
]


@pytest.fixture
def numeric_test_data_pandas_df():
    return pd.DataFrame(numeric_test_data_dict)


@pytest.fixture
def numeric_test_data_with_nulls_pandas_df():
    return pd.DataFrame(numeric_test_data_with_nulls_dict)


@pytest.fixture
def string_test_data_pandas_df():
    return pd.DataFrame(string_test_data_dict)


@pytest.fixture
def string_test_data_with_nulls_pandas_df():
    return pd.DataFrame(string_test_data_with_nulls_dict)
