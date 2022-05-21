from dataclasses import fields
from pathlib import Path

import yaml

from test_config import TestConfig
import util

def run(yamlpath: Path) -> None:
    dict_ = yaml.safe_load(util.read_file(yamlpath))
    test_config = _detect_test_config(dict_)


def _detect_test_config(yamldict: dict) -> TestConfig:
    kwargs: dict = {}

    for f in fields(TestConfig):
       kwargs[f.name] = yamldict.get(f.name)

    return TestConfig(**kwargs)


def _detect_test_cases(yamldict: dict) -> list[TestCase]:
    print(yamldict)
    pass

