from dataclasses import fields
from pathlib import Path
from typing import Any
import os

import yaml
import pandas as pd

from tdsql.test_config import TdsqlTestConfig
from tdsql.test_case import TdsqlTestCase
from tdsql.exception import TdsqlAssertionError
from tdsql.logger import logger
from tdsql import client
from tdsql import util


def main() -> None:
    # TODO parse command line arguments
    yamlpath = Path("tdsql.yaml")
    ymlpath = Path("tdsql.yml")

    if yamlpath.is_file():
        run(yamlpath)

    elif ymlpath.is_file():
        run(ymlpath)

    else:
        logger.error("tdsql.yaml is not found")
        exit(code=1)


def run(yamlpath: Path) -> None:
    test_config = _detect_test_config(yamlpath)
    test_cases = _detect_test_cases(yamlpath)
    client_ = client.get_client(test_config)
    result_dir = _make_result_dir(yamlpath.parent)

    for i, t in enumerate(test_cases):
        actual = client_.select(t.actual_sql)
        expected = client_.select(t.expected_sql)

        if test_config.auto_sort:
            actual.sort_values(
                by=list(actual.columns.values), inplace=True, ignore_index=True
            )
            expected.sort_values(
                by=list(expected.columns.values), inplace=True, ignore_index=True
            )

        if test_config.save_result:
            actual.to_csv(result_dir / f"{t.sqlpath.stem}_actual_{i}.csv", index=False)
            expected.to_csv(
                result_dir / f"{t.sqlpath.stem}_expected_{i}.csv", index=False
            )

        if test_config.ignore_column_name:
            actual_ncol = len(actual.columns)
            expected_ncol = len(expected.columns)

            if actual_ncol != expected_ncol:
                raise TdsqlAssertionError(
                    "number of columns does not match\n"
                    + f"actual: {actual_ncol}, expected {expected_ncol}"
                )

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
            if test_config.ignore_column_name:
                for c in range(actual.shape[1]):
                    actual_value = actual.iloc[i, c]
                    expected_value = expected.iloc[i, c]
                    if not _is_equal(
                        actual_value,
                        expected_value,
                        test_config.acceptable_error,
                    ):
                        raise TdsqlAssertionError(
                            f"value does not match at line: {i+1}, column: {c+1}\n"
                            + f"actual: {actual_value}, expected: {expected_value}"
                        )

            else:
                for c in actual.columns.values:
                    actual_value = actual[c][i]
                    expected_value = expected[c][i]
                    if not _is_equal(
                        actual_value,
                        expected_value,
                        test_config.acceptable_error,
                    ):
                        raise TdsqlAssertionError(
                            f"value does not match at line: {i+1}, column: {c}\n"
                            + f"actual: {actual_value}, expected: {expected_value}"
                        )

        if actual.shape[0] > expected.shape[0]:
            raise TdsqlAssertionError("actual result is longer than expected result")
        elif actual.shape[0] < expected.shape[0]:
            raise TdsqlAssertionError("expected result is longer than actual result")

        logger.info("all tests passed🎉")


def _detect_test_config(yamlpath: Path) -> TdsqlTestConfig:
    yamldict = yaml.safe_load(util.read(yamlpath))
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


def _detect_test_cases(yamlpath: Path) -> list[TdsqlTestCase]:
    yamldict = yaml.safe_load(util.read(yamlpath))
    tests = yamldict.get("tests", [])

    return [
        TdsqlTestCase(
            yamlpath.parent / t["filepath"], t.get("replace", {}), t["expected"]
        )
        for t in tests
    ]


def _make_result_dir(dir_: Path) -> Path:
    result_dir = dir_ / ".tdsql_log"
    os.makedirs(result_dir, exist_ok=True)
    util.write(result_dir / ".gitignore", "# created by tdsql\n*")
    return result_dir


def _is_equal(actual: Any, expected: Any, acceptable_error: float) -> bool:
    if type(actual) != type(expected):
        return False

    elif pd.isna(actual):
        return pd.isna(expected)

    elif isinstance(actual, float):
        res: bool = (
            expected * (1 - acceptable_error)
            <= actual
            <= expected * (1 + acceptable_error)
        )
        return res

    else:
        res = actual == expected
        return res
