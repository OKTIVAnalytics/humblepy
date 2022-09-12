from typing import TYPE_CHECKING, Union

import pandas as pd

from humblepy._utilities import _is_installed
from humblepy.validate._pyspark import _pyspark_dataframes_are_equal

if TYPE_CHECKING or _is_installed("pyspark.sql"):
    import pyspark.sql as pq


def dataframes_are_equal(
    left: Union[pd.DataFrame, pq.DataFrame],
    right: Union[pd.DataFrame, pq.DataFrame],
) -> Union[bool, None]:
    """Check that two pandas DataFrames are equal, or two PySpark DataFrames are equal.

    Args:
        left (Union[pd.DataFrame, pq.DataFrame]): First DataFrame to compare.
        right (Union[pd.DataFrame, pq.DataFrame]): Second DataFrame to compare.

    Raises:
        TypeError: If `left` and `right` are of different types.

    Returns:
        Union[bool, None]:
            True if the DataFrames are equal; False if they're not.
            None if the two objects are neither pandas DataFrames nor PySpark DataFrames.
    """
    try:
        assert type(left) is type(right)
    except:
        raise TypeError(
            f"Cannot compare DataFrames of different types. `left` is type {type(left)}; `right` is type {type(right)}."
        )

    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        return bool(left.equals(right))

    elif (
        _is_installed("pyspark.sql")
        and isinstance(left, pq.DataFrame)
        and isinstance(right, pq.DataFrame)
    ):
        return _pyspark_dataframes_are_equal(left, right)
    else:
        return None
