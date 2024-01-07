import random
import re
import time
from logging import getLogger
from typing import Self

import clickhouse_driver
import polars as pl
from aioch import Client
from api_fetcher import APIDataFetcher

logger = getLogger(__name__)


class ClickHouseTable:
    def __init__(
        self,
        table_name: str,
        client: Client,
        database: str = "default",
    ):
        self.table_name = table_name
        self.client = client
        self.database = database
        self.column_definitions: list[str] = []

    async def safe_insert_data(self, df_data: pl.DataFrame):
        if not await self.table_exists():
            await self.create_table(df_data)
        await self.insert_data(df_data)

    async def insert_data(self, df: pl.DataFrame):
        if not await self.table_exists():
            raise ValueError("Table does not exist. Please create the table first.")

        for column_name, dtype in zip(df.columns, df.dtypes):
            if "List" in str(dtype):
                fill_value = []
                df = df.with_columns(pl.col(column_name).fill_null(value=fill_value))

        await self.client.execute(
            f"INSERT INTO {self.database}.{self.table_name} VALUES",
            df.rows(),
            types_check=True,
        )

    async def table_exists(self) -> bool:
        result = await self.client.execute(
            f"SELECT 1 FROM system.tables WHERE database = '{self.database}' AND name = '{self.table_name}'"
        )
        return bool(result)

    async def create_table(self, df: pl.DataFrame):
        if await self.table_exists():
            logger.info(f"Table {self.table_name} already exists. Skipping creation.")
        else:
            await self.unsafe_create_table(df)
        if self.init_df is None:
            raise ValueError("Please create the table first.")

    async def unsafe_create_table(self, df: pl.DataFrame):
        self.init_df = df.clone()
        self.column_definitions = []
        for column_name, dtype in zip(df.columns, df.dtypes):
            clickhouse_type = str(dtype)
            clickhouse_type = clickhouse_type.replace("List", "Array")
            clickhouse_type = clickhouse_type.replace("Utf8", "String")

            column_name = str(column_name).replace(".", "_")
            if "Array" not in clickhouse_type:
                clickhouse_type = f"Nullable({clickhouse_type})"

            self.column_definitions.append(f"{column_name} {clickhouse_type}")

        create_table_query = f"CREATE TABLE {self.database}.{self.table_name} ({', '.join(self.column_definitions)}) ENGINE = MergeTree() ORDER BY tuple()"
        await self.client.execute(create_table_query)

    async def drop_table(self):
        if not await self.table_exists():
            logger.warning("Table does not exist.")
        else:
            await self.client.execute(f"DROP TABLE {self.database}.{self.table_name}")

    async def get_data(self) -> pl.DataFrame:
        if not await self.table_exists():
            raise ValueError("Table does not exist.")

        query = f"SELECT * FROM {self.database}.{self.table_name}"
        return pl.DataFrame(
            await self.client.execute(query), schema=self.init_df.schema
        )


class ClickhouseDBTables:
    _instance = None
    _TABLES: dict[str, ClickHouseTable] = {}

    def __init__(self, host: str, port: int):
        self.database = f"api_insight{random.randint(0, 10000000)}"
        self._host = host
        self._port = port
        self.client, self.sync_client = self.get_clients(self.database)

    def get_clients(self, database: str):
        client = Client(host=self._host, port=self._port, database=database)
        sync_client = clickhouse_driver.Client(
            host=self._host, port=self._port, database=database
        )
        return client, sync_client

    def reset_db(self):
        logger.info("Resetting database %s", self.database)
        _, sync_client = self.get_clients("default")
        sync_client.execute(f"DROP DATABASE IF EXISTS {self.database}")

        sync_client.execute(f"CREATE DATABASE {self.database}")

    @classmethod
    def get_instance(cls, *args, **kwargs) -> Self:
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def get_table(self, table_name: str) -> ClickHouseTable:
        if table_name not in self._TABLES:
            self._TABLES[table_name] = ClickHouseTable(
                table_name, self.client, self.database
            )
        return self._TABLES[table_name]

    async def drop_all_tables(self):
        for table_name in self._TABLES:
            await self._TABLES[table_name].drop_table()
        self._TABLES = {}

    async def table_from_api(
        self,
        api_data_fetcher: APIDataFetcher,
        table_name: str,
        path_params: dict = {},
    ):
        api_res = await api_data_fetcher.get(table_name, path_params=path_params)
        ch_table = self.get_table(table_name)
        await ch_table.create_table(api_res.data)
        await ch_table.insert_data(api_res.data)
        await ch_table.get_data()
        return ch_table, api_res.data
