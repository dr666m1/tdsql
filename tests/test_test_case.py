from pathlib import Path
import pytest

from tdsql import test_case
from tdsql import util
from tdsql.exception import InvalidInputError


@pytest.mark.parametrize(
    "sqlstr,replace,expected",
    [
        ("SELECT 1 -- tdsql-line: test", {"test": "SELECT 2"}, "SELECT 2"),
        (
            """
SELECT
-- tdsql-start: test
1 AS one
-- tdsql-end: test
""".strip(),
            {"test": "2 AS two"},
            """
SELECT
2 AS two
""".strip(),
        ),
        (
            """
-- tdsql-start: test
SELECT 1
-- tdsql-end: test
""".strip(),
            {"test": "-- tdsql-line: this\n;"},
            """
-- tdsql-start: test
SELECT 1
-- tdsql-end: test
;
""".strip(),
        ),
        (
            """
SELECT 1 UNION ALL
SELECT 2 UNION ALL
SELECT 3
""".strip(),
            {"2,3": "SELECT 4"},
            """
SELECT 1 UNION ALL
SELECT 4
""".strip(),
        ),
    ],
)
def test_replace_sql(
    sqlstr: str, replace: dict[str, str], expected: str, tmp_path: Path
) -> None:
    sqlpath = tmp_path / "tdsql.sql"
    util.write(sqlpath, sqlstr)
    replaced_spl = test_case._replace_sql(sqlpath, replace)

    assert replaced_spl == expected


@pytest.mark.parametrize(
    "msg,sqlstr,replace",
    [
        (
            r"`test` appear twice at line 4",
            """
SELECT
  1, -- tdsql-line: test
  2, -- tdsql-line: test""",
            {},
        ),
        (
            r"`test` appear twice at line 4",
            """
SELECT
  1, -- tdsql-start: test
  2, -- tdsql-start: test""",
            {},
        ),
        (
            r"`test` has not started but ends at line 2",
            """
SELECT 1 -- tdsql-end: test""",
            {},
        ),
        (
            r"`test` started at line 2 but it does not end",
            """
SELECT 1 -- tdsql-start: test""",
            {},
        ),
        (
            r"`test2` does not appear",
            """
SELECT 1 -- tdsql-line: test1""",
            {"test2": ""},
        ),
        (
            r"cannot replace line 3 twice",
            """
-- tdsql-start: test1
-- tdsql-start: test2
SELECT
-- tdsql-end: test2
-- tdsql-end: test1""",
            {"test1": "", "test2": ""},
        ),
        (
            r"only `-- tdsql-line: this` is allowed but got `test`",
            "SELECT 1 -- tdsql-line: test",
            {"test": "-- tdsql-line: test"},
        ),
    ],
)
def test_replace_sql_err(
    msg: str, sqlstr: str, replace: dict[str, str], tmp_path: Path
) -> None:
    sqlpath = tmp_path / "tdsql.sql"
    util.write(sqlpath, sqlstr)

    with pytest.raises(InvalidInputError, match=msg):
        test_case._replace_sql(sqlpath, replace)
