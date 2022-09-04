import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from humblepy.transform.humble_pandas import _get_pandas_hash_key_column


def test_get_pandas_hash_key_column_sha256(numeric_with_nulls_pandas_df_fixture):
    """Tests the _get_pandas_hash_key_column() function using its default hash algorithm (SHA-256), concatenating strings with the default '||' and replacing nulls with the default '^^'.

    Args:
        numeric_with_nulls_pandas_df_fixture (callable): pytest fixture for a list of dicts containing mostly numeric test data, with some null (NoneType) values, to create a DataFrame with.

    """
    assert_frame_equal(
        _get_pandas_hash_key_column(
            numeric_with_nulls_pandas_df_fixture,
            columns_to_hash=["a", "b"],
        ),
        pd.DataFrame(
            {
                "hash_key": [
                    "b0397606ceafa892f0a0ddf6d0f945630c6b85ce7448889e9094bcc65239b358",
                    "067d28a6f3a68ee32706a0f00fb70bce8e200e13943610a2b9cf4542e3ecfa19",
                ]
            }
        ),
    )


def test_get_pandas_hash_key_column_sha512(numeric_pandas_df_fixture):
    """Tests the _get_pandas_hash_key_column() function using the SHA-512 hash algorithm, concatenating strings with the default '||' and replacing nulls with the default '^^'.

    Args:
        numeric_pandas_df_fixture (callable): pytest fixture for a list of dicts containing numeric test data, to create a DataFrame with.
    """

    assert_frame_equal(
        _get_pandas_hash_key_column(
            pd.DataFrame(numeric_pandas_df_fixture),
            columns_to_hash=["a", "b"],
            hash_algorithm="sha512",
        ),
        pd.DataFrame(
            {
                "hash_key": [
                    "2841a969f3d1a57b8316772a8e731a6571524c8124f388007d8af599b63fe2cbf7cb04896ece7f7fa5c5e607b1277bb93ead53d1a8ec60624c8f6eddcbb3f58f",
                    "c080aeb6c24e62a6ee05c522deafc81f1abbf5338307ad1b350da889fc8a0ef2a964bf570f5ef769ecd132f21933dd24da8e714b484bbd65a01feec7a75386fc",
                ]
            }
        ),
    )


def test_get_pandas_hash_key_column_md5(string_with_nulls_pandas_df_fixture):
    """Tests the _get_pandas_hash_key_column() function using the MD5 hash algorithm, concatenating strings with '-' and replacing nulls with '#'.

    Args:
        string_with_nulls_pandas_df_fixture (callable): pytest fixture for a list of dicts containing mostly string test data, with some null (NoneType) values, to create a DataFrame with.
    """

    assert_frame_equal(
        _get_pandas_hash_key_column(
            pd.DataFrame(string_with_nulls_pandas_df_fixture),
            columns_to_hash=["a", "b"],
            hash_algorithm="md5",
            concat_with="-",
            replace_null_with="#",
        ),
        pd.DataFrame(
            {
                "hash_key": [
                    "930511fa326b6bca3b817ed81cfc2407",
                    "a7a8cced4abd6e8f983a133f6ddc84a5",
                ]
            }
        ),
    )
