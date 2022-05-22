from dataclasses import fields
from pathlib import Path
from typing import Any
import os

import yaml

from tdsql.test_config import TdsqlTestConfig
from tdsql.test_case import TdsqlTestCase
from tdsql.exception import TdsqlAssertionError
from tdsql.logger import logger
from tdsql import client
from tdsql import util


def run(yamlpath: Path) -> None:
    dict_ = yaml.safe_load(util.read_file(yamlpath))
    test_config = _detect_test_config(dict_)
    test_cases = _detect_test_cases(dict_)
    client_ = client.get_client(test_config)
    result_dir = _make_result_dir()

    for i, t in enumerate(test_cases):
        actual = client_.select(t.actual_sql)
        expected = client_.select(t.expected_sql)

        if test_config.auto_sort:
            actual.sort_values(
                by=list(actual.columns.values),
                inplace=True,
                ignore_index=True
            )
            expected.sort_values(
                by=list(actual.columns.values),
                inplace=True,
                ignore_index=True
            )

        if test_config.save_result:
            actual.to_csv(result_dir / f"{t.sqlpath.stem}_actual_{i}.csv")
            expected.to_csv(result_dir / f"{t.sqlpath.stem}_expected_{i}.csv")

        if test_config.ignore_column_name:
            actual_ncol = len(actual.columns)
            expected_ncol = len(expected.columns)

            if actual_ncol != expected_ncol:
                raise TdsqlAssertionError(f"""number of columns does not match
actual: {actual_ncol}, expected {expected_ncol}""")

            else:
                actual_column_set = set(actual.columns.values)
                expected_column_set = set(expected.columns.values)

                actual_only_set = actual_column_set - expected_column_set
                expected_only_set = expected_column_set - actual_column_set

                if len(actual_only_set) > 0:
                    raise TdsqlAssertionError(
                        f"{actual_only_set} only exsists in actual result"
                    )
                elif len(expected_only_set) > 0:
                    raise TdsqlAssertionError(
                        f"{expected_only_set} only exsists in expected result"
                    )

        for i in range(min(actual.shape[0], expected.shape[0])):
            actual_row = actual.iloc[i:i+1]
            expected_row = expected.iloc[i:i+1]

            if test_config.ignore_column_name:
                for i in range(actual.shape[1]):
                    actual_value = actual_row.iloc[0,i]
                    expected_value = expected_row.iloc[0,i]
                    if not _is_equal(
                        actual_value,
                        expected_value,
                        test_config.acceptable_error,
                    ):
                        raise TdsqlAssertionError(f"at {i+1}th column")
            else:
                for c in actual.columns.values:
                    actual_value = actual_row[c][0]
                    expected_value = expected_row[c][0]
                    if not _is_equal(
                        actual_value,
                        expected_value,
                        test_config.acceptable_error,
                    ):
                        raise TdsqlAssertionError(f"at {i+1}th column")

        if actual.shape[0] > expected.shape[0]:
            raise TdsqlAssertionError("actual result is longer than expected result")
        elif actual.shape[0] < expected.shape[0]:
            raise TdsqlAssertionError("expected result is longer than actual result")

        logger.info("all tests passed")


def _detect_test_config(yamldict: dict[str, Any]) -> TdsqlTestConfig:
    kwargs: dict[str, Any] = {}

    for f in fields(TdsqlTestConfig):
        val = yamldict.get(f.name)
        if val is None:
            pass

        elif f.type == type(val):
            kwargs[f.name] = val

        else:
            try:
                kwargs[f.name] = f.type(val)
            except ValueError:
                kwargs[f.name] = f.type(eval(val))

    return TdsqlTestConfig(**kwargs)


def _detect_test_cases(yamldict: dict[str, Any]) -> list[TdsqlTestCase]:
    tests = yamldict.get("tests", [])
    return [TdsqlTestCase(t["filepath"], t["replace"]) for t in tests]


def _make_result_dir() -> Path:
    dir_ = Path(".tdsql_log")
    os.makedirs(dir_, exist_ok=True)

    with open(dir_ / ".gitignore", "w") as f:
        f.write("# created by tdsql\n*")

    return dir_


def _is_equal(actual: Any, expected: Any, acceptable_error: float) -> bool:
    if type(actual) != type(expected):
        return False

    if isinstance(actual, float):
        return (
            expected * (1 - acceptable_error) <= actual
            and actual <= expected * (1 + acceptable_error)
        )

    return actual == expected
