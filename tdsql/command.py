from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import fields
from pathlib import Path
from typing import Any, Literal
import os
import sys

import yaml
import pandas as pd

from tdsql.test_config import TdsqlTestConfig
from tdsql.test_case import TdsqlTestCase
from tdsql.exception import TdsqlAssertionError, TdsqlInternalError
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
        sys.exit(1)


def run(yamlpath: Path) -> None:
    test_config = _detect_test_config(yamlpath)
    test_cases = _detect_test_cases(yamlpath)
    client_ = client.get_client(test_config)
    result_dir = _make_result_dir(yamlpath.parent)

    # exec query
    with ThreadPoolExecutor(max_workers=test_config.max_threads) as pool:
        futures: dict[
            tuple[int, Literal["actual", "expected"]], Future[pd.DataFrame]
        ] = {}

        for t in test_cases:
            futures[(t.id, "actual")] = pool.submit(client_.select, t.actual_sql)
            futures[(t.id, "expected")] = pool.submit(client_.select, t.expected_sql)

        for t in test_cases:
            try:
                actual = futures[(t.id, "actual")].result()
                actual.to_csv(
                    result_dir / f"{t.sqlpath.stem}_{t.id}_actual.csv", index=False
                )
                t.actual_sql_result = actual
            except Exception as e:
                t.actual_sql_result = e

            try:
                expected = futures[(t.id, "expected")].result()
                expected.to_csv(
                    result_dir / f"{t.sqlpath.stem}_{t.id}_expected.csv", index=False
                )
                t.expected_sql_result = expected
            except Exception as e:
                t.expected_sql_result = e

    # compare results
    pass_count = 0
    fail_count = 0
    errors: list[TdsqlAssertionError] = []

    for t in test_cases:
        try:
            _compare_results(t, test_config)
            pass_count += 1
        except TdsqlAssertionError as e:
            errors.append(e)
            fail_count += 1

    for err in errors:
        logger.error(err)

    logger.info(f"{pass_count} tests passed, {fail_count} tests failed")

    if fail_count > 0:
        sys.exit(1)


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


def _compare_results(test: TdsqlTestCase, config: TdsqlTestConfig) -> None:
    if test.actual_sql_result is None or test.expected_sql_result is None:
        raise TdsqlInternalError()

    if isinstance(test.actual_sql_result, Exception):
        raise TdsqlAssertionError(
            f"{test.sqlpath}_{test.id}: invalid query\n"
            + f"{test.actual_sql}\n{test.expected_sql_result}"
        )

    elif isinstance(test.expected_sql_result, Exception):
        raise TdsqlAssertionError(
            f"{test.sqlpath}_{test.id}: invalid query\n"
            + f"{test.expected_sql}\n{test.expected_sql_result}"
        )

    if config.auto_sort:
        test.actual_sql_result.sort_values(
            by=list(test.actual_sql_result.columns.values),
            inplace=True,
            ignore_index=True,
        )
        test.expected_sql_result.sort_values(
            by=list(test.expected_sql_result.columns.values),
            inplace=True,
            ignore_index=True,
        )

    if config.ignore_column_name:
        actual_ncol = len(test.actual_sql_result.columns)
        expected_ncol = len(test.expected_sql_result.columns)

        if actual_ncol != expected_ncol:
            raise TdsqlAssertionError(
                f"{test.sqlpath}_{test.id}: number of columns does not match\n"
                + f"actual: {actual_ncol}, expected {expected_ncol}"
            )

    else:
        actual_column_set = set(test.actual_sql_result.columns.values)
        expected_column_set = set(test.expected_sql_result.columns.values)

        actual_only_set = actual_column_set - expected_column_set
        expected_only_set = expected_column_set - actual_column_set

        if len(actual_only_set) > 0:
            raise TdsqlAssertionError(
                f"{test.sqlpath}_{test.id}: "
                + f"{actual_only_set} only exsists in actual result"
            )
        elif len(expected_only_set) > 0:
            raise TdsqlAssertionError(
                f"{test.sqlpath}_{test.id}: "
                + f"{expected_only_set} only exsists in expected result"
            )

    for i in range(
        min(test.actual_sql_result.shape[0], test.expected_sql_result.shape[0])
    ):
        if config.ignore_column_name:
            for c in range(test.actual_sql_result.shape[1]):
                actual_value = test.actual_sql_result.iloc[i, c]
                expected_value = test.expected_sql_result.iloc[i, c]
                if not _is_equal(
                    actual_value,
                    expected_value,
                    config.acceptable_error,
                ):
                    raise TdsqlAssertionError(
                        f"{test.sqlpath}_{test.id}: value does not match "
                        + f"at line: {i+1}, column: {c+1}\n"
                        + f"actual: {actual_value}, expected: {expected_value}"
                    )

        else:
            for c in test.actual_sql_result.columns.values:
                actual_value = test.actual_sql_result[c][i]
                expected_value = test.expected_sql_result[c][i]
                if not _is_equal(
                    actual_value,
                    expected_value,
                    config.acceptable_error,
                ):
                    raise TdsqlAssertionError(
                        f"{test.sqlpath}_{test.id}: value does not match "
                        + f"at line: {i+1}, column: {c}\n"
                        + f"actual: {actual_value}, expected: {expected_value}"
                    )

    if test.actual_sql_result.shape[0] > test.expected_sql_result.shape[0]:
        raise TdsqlAssertionError(
            f"{test.sqlpath}_{test.id}: actual result is longer than expected result"
        )
    elif test.actual_sql_result.shape[0] < test.expected_sql_result.shape[0]:
        raise TdsqlAssertionError(
            f"{test.sqlpath}_{test.id}: expected result is longer than actual result"
        )


def _make_result_dir(dir_: Path) -> Path:
    result_dir = dir_ / ".tdsql_log"
    os.makedirs(result_dir, exist_ok=True)
    util.write(result_dir / ".gitignore", "# created by tdsql\n*")
    return result_dir


def _is_equal(actual: Any, expected: Any, acceptable_error: float) -> bool:
    res: bool

    if type(actual) != type(expected):
        res = False

    elif pd.isna(actual):
        res = pd.isna(expected)

    elif isinstance(actual, float):
        res = (
            expected * (1 - acceptable_error)
            <= actual
            <= expected * (1 + acceptable_error)
        )

    else:
        res = actual == expected

    return res
