import pytest

from tdsql import client
from tdsql.test_config import TdsqlTestConfig


@pytest.mark.parametrize(
    "database",
    ["bigquery"],
)
def test_select(database: str) -> None:
    client_ = client.get_client(database=database)
    df = client_.select("SELECT 1 AS i;", TdsqlTestConfig(database=database))

    assert df["i"].values[0] == 1
