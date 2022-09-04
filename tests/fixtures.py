import pytest


@pytest.fixture
def get_numeric_test_data_list():
    """Returns a list of dicts containing numeric test data, to create a DataFrame with.

    Returns:
        list: A list of dicts with column names as keys and integers as values.
    """
    return [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]


@pytest.fixture
def get_numeric_test_data_with_nulls_list():
    """Returns a list of dicts containing mostly numeric test data, with some null (NoneType) values, to create a DataFrame with.

    Returns:
        list: A list of dicts with column names as keys, and integers and NoneTypes as values.
    """
    return [
        {"a": None, "b": 2, "c": None},
        {"a": 10, "b": None, "c": 30},
    ]


@pytest.fixture
def get_string_test_data_list():
    """Returns a list of dicts containing string test data, to create a DataFrame with.

    Returns:
        list: A list of dicts with column names as keys and strings as values.
    """
    return [
        {"a": "apple", "b": "banana", "c": "cherry"},
        {"a": "avocado", "b": "blueberry", "c": "clementine"},
    ]


@pytest.fixture
def get_string_test_data_with_nulls_list():
    """Returns a list of dicts containing mostly string test data, with some null (NoneType) values, to create a DataFrame with.

    Returns:
        list: A list of dicts with column names as keys, and strings and NoneTypes as values.
    """
    return [
        {"a": None, "b": "banana", "c": None},
        {"a": "avocado", "b": None, "c": "clementine"},
    ]
