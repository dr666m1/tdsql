from pathlib import Path
import tempfile

import pytest

from tdsql.test_config import TdsqlTestConfig
from tdsql.exception import TdsqlAssertionError
from tdsql import command
from tdsql import util


@pytest.mark.parametrize(
    "yamlstr,expected",
    [
        (
            # minimum
            """
database: bigquery
""",
            TdsqlTestConfig(database="bigquery"),
        ),
        (
            # arithmetic operation
            """
database: bigquery
max_bytes_billed: '1024 ** 3'
""",
            TdsqlTestConfig(database="bigquery", max_bytes_billed=1024**3),
        ),
    ],
)
def test_detect_test_config(yamlstr: str, expected: TdsqlTestConfig) -> None:
    with tempfile.NamedTemporaryFile(mode="w") as f:
        f.write(yamlstr)
        f.seek(0)

        actual = command._detect_test_config(Path(f.name))
        assert actual == expected


@pytest.mark.parametrize(
    "msg,yamlstr,sqlstr",
    [
        # most simple
        (
            r"value does not match at line: 1, column: 1\n" + r"actual: 2, expected: 1",
            """
database: bigquery
ignore_column_name: true
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1
""",
            """
SELECT 2
""",
        ),
        # auto_sort
        (
            r"value does not match at line: 1, column: num\n"
            + r"actual: 2, expected: 1",
            """
database: bigquery
auto_sort: false
tests:
  - filepath: ./tdsql.sql
    expected: |
      SELECT 1 AS num UNION ALL
      SELECT 2
""",
            """
SELECT 2 AS num UNION ALL
SELECT 1
""",
        ),
        # column does not match
        (
            r"""number of columns does not match
actual: 2, expected 1""",
            """
database: bigquery
ignore_column_name: true
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1
""",
            """
SELECT 1, 2
""",
        ),
        (
            r"\{'two'\} only exsists in actual result",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1 AS one
""",
            """
SELECT 1 AS one, 2 AS two
""",
        ),
        (
            r"\{'two'\} only exsists in expected result",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1 AS one, 2 AS two
""",
            """
SELECT 1 AS one
""",
        ),
        # equality
        (
            r"value does not match at line: 1, column: one\n"
            + r"actual: 1, expected: 1.0",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1.0 AS one
""",
            """
SELECT 1 AS one -- type does not match
""",
        ),
        (
            r"value does not match at line: 1, column: col\n"
            + r"actual: 1.0, expected: 1.002",
            """
database: bigquery
acceptable_error: 1.0e-3
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1.001 AS col
  - filepath: ./tdsql.sql
    expected: SELECT 1.002 AS col
""",
            """
SELECT 1.0 AS col
""",
        ),
        # number of rows does not match
        (
            "actual result is longer than expected result",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1 AS col
""",
            """
SELECT 1 AS col UNION ALL
SELECT 2
""",
        ),
        (
            "expected result is longer than actual result",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: |
      SELECT 1 AS col UNION ALL
      SELECT 2
""",
            """
SELECT 1 AS col
""",
        ),
    ],
)
def test_run_err(msg: str, yamlstr: str, sqlstr: str) -> None:
    with tempfile.TemporaryDirectory() as dirname:
        util.write(Path(dirname) / "tdsql.yaml", yamlstr)
        util.write(Path(dirname) / "tdsql.sql", sqlstr)

        with pytest.raises(TdsqlAssertionError, match=msg):
            command.run(Path(dirname) / "tdsql.yaml")
