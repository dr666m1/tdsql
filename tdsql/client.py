from abc import ABC, abstractmethod

import pandas as pd

from tdsql.exception import InvalidYaml
from tdsql.test_config import TdsqlTestConfig

class BaseClient(ABC):
    @abstractmethod
    def select(self, sql: str, config: TdsqlTestConfig) -> pd.DataFrame:
        pass


class BigQueryClient(BaseClient):
    def __init__(self):
        pass

    def select(self, sql: str, config: TdsqlTestConfig) -> pd.DataFrame:
        print(sql, config)
        return pd.DataFrame()


def get_client(database: str) -> BaseClient:
    if database == "bigquery":
        return BigQueryClient()
    else:
        raise InvalidYaml(f"{database} is not supported")
