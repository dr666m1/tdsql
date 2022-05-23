import pytest

from tdsql import client
from tdsql.test_config import TdsqlTestConfig


@pytest.mark.parametrize(
    "database",
    ["bigquery"],
)
def test_select(database: str):
    client_ = client.get_client(TdsqlTestConfig(database=database))
    df = client_.select("SELECT 1 AS i;")

    assert df["i"].values[0] == 1
