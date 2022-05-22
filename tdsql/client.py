from abc import ABC, abstractmethod

from google.cloud import bigquery
import pandas as pd

from tdsql.exception import InvalidYaml
from tdsql.test_config import TdsqlTestConfig

class BaseClient(ABC):
    @abstractmethod
    def select(self, sql: str) -> pd.DataFrame:
        pass


class BigQueryClient(BaseClient):
    def __init__(self, config: TdsqlTestConfig) -> None:
        self.config = config

        query_job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=config.max_bytes_billed,
            use_legacy_sql=False,
        )

        # See https://googleapis.dev/python/google-api-core/latest/auth.html#authentication
        self.client = bigquery.Client(
            default_query_job_config=query_job_config,
        )

    def select(self, sql: str) -> pd.DataFrame:
        return self.client.query(sql).to_dataframe()


def get_client(config: TdsqlTestConfig) -> BaseClient:
    if config.database == "bigquery":
        return BigQueryClient(config)
    else:
        raise InvalidYaml(f"{config.database} is not supported")
