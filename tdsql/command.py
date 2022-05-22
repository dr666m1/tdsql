from dataclasses import fields
from pathlib import Path
from typing import Any

import yaml

from tdsql.test_config import TdsqlTestConfig
from tdsql.test_case import TdsqlTestCase
from tdsql import client
from tdsql import util


def run(yamlpath: Path) -> None:
    dict_ = yaml.safe_load(util.read_file(yamlpath))
    test_config = _detect_test_config(dict_)
    test_cases = _detect_test_cases(dict_)
    client_ = client.get_client(test_config)

    for t in test_cases:
        actual = client_.select(t.actual_sql)
        expected = client_.select(t.expected_sql)

        print(actual, expected)


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

