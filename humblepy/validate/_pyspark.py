from humblepy._utilities import _import_optional_dependency

_import_optional_dependency("pyspark")


import pyspark.sql as pq


def _pyspark_dataframes_are_equal(left: pq.DataFrame, right: pq.DataFrame) -> bool:
    """Check that two PySpark DataFrames are equal.

    Args:
        left (pq.DataFrame): First DataFrame to compare.
        right (pq.DataFrame): Second DataFrame to compare.

    Returns:
        bool: True if the schema and data in the DataFrames are equal, else False.
    """
    return bool(left.schema == right.schema and left.collect() == right.collect())
