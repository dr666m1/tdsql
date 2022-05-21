from pathlib import Path
from tempfile import NamedTemporaryFile
import pytest

from tdsql import test_case

@pytest.mark.parametrize(
    "sql,replace,expected",
    [
        (
            "SELECT 1 -- tdsql-line: test",
            {"test": "SELECT 2"},
            "SELECT 2"
        ),
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
""".strip()
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
    ]
)
def test_replace_sql(sql: str, replace: dict[str, str], expected: str):
    with NamedTemporaryFile(mode="w") as f:
        f.write(sql)
        f.seek(0)
        replaced_spl = test_case._replace_sql(Path(f.name), replace)

    assert replaced_spl == expected

