from typing import Self

import pandas as pd
from api_fetcher import DomotzAPIDataFetcher
from api_fetcher.cache.memory import InMemoryCache
from api_fetcher.settings import BASE_URLS, DomotzAPISettings
from clickhouse_driver import Client
from dotenv import dotenv_values
from httpx import delete

env = dotenv_values("/workspaces/public-api-insight/.env")


class ClickHouseTable:
    _pandas_sql_dtype_mapping = {
        "int64": "Int64",
        "float64": "Float64",
        "object": "String",
        "bool": "Boolean",
    }

    def __init__(self, table_name: str, client: Client, database: str = "default"):
        self.table_name = table_name
        self.client = client
        self.database = database
        self.column_definitions: list[str] = []

    def insert_data(self, df_data: pd.DataFrame):
        if not self.table_exists():
            raise ValueError("Table does not exist. Please create the table first.")

        df_data = df_data.where(pd.notnull(df_data), None)
        data_tuples = list(df_data.itertuples(index=False, name=None))

        self.client.execute(
            f"INSERT INTO {self.database}.{self.table_name} VALUES",
            data_tuples,
            types_check=True,
        )

    def table_exists(self) -> bool:
        result = self.client.execute(
            f"SELECT 1 FROM system.tables WHERE database = '{self.database}' AND name = '{self.table_name}'"
        )
        return bool(result)


class ClickhouseDBTables:
    _instance = None
    _TABLES: dict[str, ClickHouseTable] = {}

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.client = Client(
            host=host, port=port, user=user, password=password, database=database
        )

    @classmethod
    def get_instance(cls, *args, **kwargs) -> Self:
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def get_table(self, table_name: str) -> ClickHouseTable:
        if table_name not in self._TABLES:
            self._TABLES[table_name] = ClickHouseTable(table_name, self.client)
        return self._TABLES[table_name]
