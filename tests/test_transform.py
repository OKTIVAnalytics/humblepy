import pandas as pd
import pytest

from humblepy.transform.functions import get_hash_key_column
from tests.fixtures import get_numeric_test_data_with_nulls_list


def test_get_hash_key_column_pandas(get_numeric_test_data_with_nulls_list):
    """Tests the get_hash_key_column() to make sure it returns a pandas DataFrame if a pandas DataFrame is passed in.

    Args:
        get_numeric_test_data_with_nulls_list (callable): pytest fixture for a pandas DataFrame containing mostly numeric values, with some missing values (NaNs).
    """
    assert isinstance(
        get_hash_key_column(
            pd.DataFrame(get_numeric_test_data_with_nulls_list),
            columns_to_hash=["a", "b"],
        ),
        pd.DataFrame,
    )
