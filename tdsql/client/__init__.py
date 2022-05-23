from tdsql.test_config import TdsqlTestConfig
from tdsql.exception import InvalidInputError
from tdsql.client.base import BaseClient


def get_client(config: TdsqlTestConfig) -> BaseClient:
    if config.database == "bigquery":
        from tdsql.client import bigquery

        return bigquery.BigQueryClient(config)
    else:
        raise InvalidInputError(f"{config.database} is not supported")
