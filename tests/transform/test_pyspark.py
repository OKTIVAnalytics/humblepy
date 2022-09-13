import pytest

from humblepy.transform._pyspark import _with_pyspark_hash_column


def test_with_pyspark_hash_column_sha256(numeric_with_nulls_pyspark_df_fixture):
    """Tests the _with_pyspark_hash_column() function using its default hash algorithm (SHA-256), concatenating strings with the default '||' and replacing nulls with the default '^^'.

    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing mostly numeric test data, with some null (NoneType) values.

    """

    assert _with_pyspark_hash_column(
        numeric_with_nulls_pyspark_df_fixture,
        columns_to_hash=["a", "b"],
        hash_algorithm="sha256",
        is_key=False,
        sort_columns=False,
        concat_with="||",
        replace_null_with="^^",
        uppercase=True,
        strip_whitespace=True,
        hash_column_name="hash_value",
    ).select("hash_value").rdd.flatMap(tuple).collect() == [
        "2aecd63a78bb594cc3a97782f95314fd09260958fe1dc61d17e57cbe4517afad",
        "d36106ae37598e73f4547235aedc203811d32b99970e711d8bd1d0dab0e9c496",
    ]


def test_with_pyspark_hash_column_sha512(numeric_pyspark_df_fixture):
    """Tests the _with_pyspark_hash_column() function using the SHA-512 hash algorithm, concatenating strings with the default '||' and replacing nulls with the default '^^'.

    Args:
        numeric_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing numeric test data.
    """

    assert _with_pyspark_hash_column(
        numeric_pyspark_df_fixture,
        columns_to_hash=["a", "b"],
        hash_algorithm="sha512",
        is_key=False,
        sort_columns=False,
        concat_with="||",
        replace_null_with="^^",
        uppercase=True,
        strip_whitespace=True,
        hash_column_name="hash_value",
    ).select("hash_value").rdd.flatMap(tuple).collect() == [
        "2841a969f3d1a57b8316772a8e731a6571524c8124f388007d8af599b63fe2cbf7cb04896ece7f7fa5c5e607b1277bb93ead53d1a8ec60624c8f6eddcbb3f58f",
        "c080aeb6c24e62a6ee05c522deafc81f1abbf5338307ad1b350da889fc8a0ef2a964bf570f5ef769ecd132f21933dd24da8e714b484bbd65a01feec7a75386fc",
    ]


def test_with_pyspark_hash_column_md5(string_with_nulls_pyspark_df_fixture):
    """Tests the _with_pyspark_hash_column() function using the MD5 hash algorithm, concatenating strings with '-' and replacing nulls with '#'.

    Args:
        string_with_nulls_pyspark_df_fixture (callable): pytest fixture for a PySpark DataFrame containing mostly string test data, with some null (NoneType) values.
    """

    assert _with_pyspark_hash_column(
        string_with_nulls_pyspark_df_fixture,
        columns_to_hash=["a", "b"],
        hash_algorithm="md5",
        is_key=False,
        sort_columns=False,
        concat_with="-",
        replace_null_with="#",
        uppercase=False,
        strip_whitespace=True,
        hash_column_name="hash_value",
    ).select("hash_value").rdd.flatMap(tuple).collect() == [
        "930511fa326b6bca3b817ed81cfc2407",
        "a7a8cced4abd6e8f983a133f6ddc84a5",
    ]


def test_with_pyspark_hash_column_is_key(numeric_with_nulls_pyspark_df_fixture):
    """Tests the _with_pyspark_hash_column() function to make sure it returns a null value for a row if `is_key` is True and the columns being hashed all contain null values for that row.

    Args:
        numeric_with_nulls_pyspark_df_fixture (callable): pytest fixture for a list of dicts containing mostly numeric test data, with some null (NoneType) values, to create a DataFrame with.

    """

    assert _with_pyspark_hash_column(
        numeric_with_nulls_pyspark_df_fixture,
        columns_to_hash=["a"],
        hash_algorithm="sha256",
        is_key=True,
        sort_columns=False,
        concat_with="||",
        replace_null_with="^^",
        uppercase=True,
        strip_whitespace=True,
        hash_column_name="hash_value",
    ).select("hash_value").rdd.flatMap(tuple).collect() == [
        None,
        "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
    ]
