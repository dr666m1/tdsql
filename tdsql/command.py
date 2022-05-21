from dataclasses import fields
from pathlib import Path

import yaml

from tdsql.test_config import TdsqlTestConfig
from tdsql.test_case import TdsqlTestCase
from tdsql import util

def run(yamlpath: Path) -> None:
    dict_ = yaml.safe_load(util.read_file(yamlpath))
    test_config = _detect_test_config(dict_)
    test_cases = _detect_test_cases(dict_)
    for t in test_cases:
        t.run("client here!", test_config)


def _detect_test_config(yamldict: dict) -> TdsqlTestConfig:
    kwargs: dict = {}

    for f in fields(TdsqlTestConfig):
        val = yamldict.get(f.name)
        if val is None:
            continue
        if f.type == type(val):
            kwargs[f.name] = val
        elif callable(f.type):
            try:
                kwargs[f.name] = f.type(val)
            except ValueError:
                kwargs[f.name] = f.type(eval(val))

    return TdsqlTestConfig(**kwargs)


def _detect_test_cases(yamldict: dict) -> list[TdsqlTestCase]:
    tests = yamldict.get("tests", [])
    return [TdsqlTestCase(t["filepath"], t["replace"]) for t in tests]

