from pathlib import Path

import pytest

from tdsql.test_config import TdsqlTestConfig
from tdsql.exception import InvalidInputError, TdsqlAssertionError
from tdsql import command
from tdsql import util
from tdsql import client


@pytest.mark.parametrize(
    "yamlstr,expected,parent_config",
    [
        (
            # minimum
            """
database: bigquery
""",
            TdsqlTestConfig(database="bigquery"),
            None,
        ),
        (
            # arithmetic operation
            """
database: bigquery
max_bytes_billed: '1024 ** 3'
""",
            TdsqlTestConfig(database="bigquery", max_bytes_billed=1024**3),
            None,
        ),
        (
            # inheritance
            """
database: bigquery
""",
            TdsqlTestConfig(database="bigquery", max_bytes_billed=0),
            TdsqlTestConfig(database="foo", max_bytes_billed=0),
        ),
    ],
)
def test_detect_test_config(
    yamlstr: str,
    expected: TdsqlTestConfig,
    parent_config: TdsqlTestConfig,
    tmp_path: Path,
) -> None:
    yamlpath = tmp_path / "tdsql.yaml"
    util.write(yamlpath, yamlstr)

    actual = command._detect_test_config(yamlpath, parent_config)
    assert actual == expected


@pytest.mark.parametrize(
    "msg,yamlstr,sqlstr",
    [
        # most simple
        (
            r"value does not match at line: 1, column: 1\nactual: 2, expected: 1",
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
        (
            r"value does not match at line: 2, column: 1\nactual: 3, expected: 2",
            """
database: bigquery
ignore_column_name: true
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1 UNION ALL SELECT 2
""",
            """
SELECT 1 UNION ALL SELECT 3
""",
        ),
        # invalid query
        (
            r"invalid query\nSELECT foo",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT foo
""",
            """
SELECT 1
""",
        ),
        (
            r"invalid query\nSELECT foo",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT 1
""",
            "SELECT foo",
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
        (
            r"value does not match at line: 1, column: col\n"
            + r"actual: 1, expected: <NA>",
            """
database: bigquery
tests:
  - filepath: ./tdsql.sql
    expected: SELECT NULL AS col
""",
            """
SELECT 1 AS col
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
def test_compare_results(msg: str, yamlstr: str, sqlstr: str, tmp_path: Path) -> None:
    yamlpath = Path(tmp_path) / "tdsql.yaml"
    sqlpath = Path(tmp_path) / "tdsql.sql"

    util.write(yamlpath, yamlstr)
    util.write(sqlpath, sqlstr)

    test_config = command._detect_test_config(yamlpath)
    test_cases = command._detect_test_cases(yamlpath)
    client_ = client.get_client(test_config.database)

    with pytest.raises(TdsqlAssertionError, match=msg):
        for t in test_cases:
            try:
                t.actual_sql_result = client_.select(t.actual_sql, test_config)
            except Exception as e:
                t.actual_sql_result = e
            try:
                t.expected_sql_result = client_.select(t.expected_sql, test_config)
            except Exception as e:
                t.expected_sql_result = e

            command._compare_results(t, test_config)


@pytest.mark.parametrize(
    "files,expected",
    [
        ({"tdsql.yaml": "database: foo"}, ["tdsql.yaml"]),
        (
            {
                "tdsql.yaml": "database: foo\nsource: ./childs/child1.yaml",
                "childs/child1.yaml": """
source:
  - ./child2.yaml
  - child3.yml
""",
                "childs/child2.yaml": "source:",
                "childs/child3.yml": "foo: bar",
            },
            [
                "tdsql.yaml",
                "childs/child1.yaml",
                "childs/child2.yaml",
                "childs/child3.yml",
            ],
        ),
        # glob
        (
            {
                "tdsql.yaml": "database: foo\nsource: '*.yaml'",
                "child1.yaml": "foo: bar",
                "child2.yaml": "foo: bar",
            },
            [
                "tdsql.yaml",
                "child1.yaml",
                "child2.yaml",
            ],
        ),
    ],
)
def test_parse_root_yaml(
    files: dict[str, str], expected: list[str], tmp_path: Path
) -> None:
    for k, v in files.items():
        util.write(tmp_path / k, v)

    test_config_cases = command._parse_root_yaml(tmp_path / "tdsql.yaml")
    actual_set = {yaml_ for yaml_ in test_config_cases.keys()}
    expected_set = {(tmp_path / yaml_).resolve() for yaml_ in expected}

    assert actual_set == expected_set


@pytest.mark.parametrize(
    "msg,files",
    [
        (
            "detected circular reference",
            {
                "tdsql.yaml": """database: foo\nsource: ./childs/child1.yaml""",
                "childs/child1.yaml": "source: ../tdsql.yaml",
            },
        ),
    ],
)
def test_parse_root_yaml_err(msg: str, files: dict[str, str], tmp_path: Path) -> None:
    for k, v in files.items():
        util.write(tmp_path / k, v)

    with pytest.raises(InvalidInputError, match=msg):
        command._parse_root_yaml(tmp_path / "tdsql.yaml")
