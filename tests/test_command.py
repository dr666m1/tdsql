import pytest
import yaml

from tdsql.test_config import TdsqlTestConfig
from tdsql import command

@pytest.mark.parametrize(
    "yamlstr,expected",
    [
        (
            # minimum
            """
database: bigquery
""",
            TdsqlTestConfig(database="bigquery")
        ),
        (
            # arithmetic operation
            """
database: bigquery
max_bytes_billed: '1024 ** 3'
""",
            TdsqlTestConfig(
                database="bigquery",
                max_bytes_billed=1024 ** 3
            )
        ),
    ]
)
def test_detect_test_config(yamlstr: str, expected: TdsqlTestConfig):
    dict_ = yaml.safe_load(yamlstr)
    actual = command._detect_test_config(dict_)
    assert actual == expected

