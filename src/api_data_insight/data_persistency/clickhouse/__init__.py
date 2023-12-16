from logging import getLogger
from textwrap import fill
from typing import Self

import pandas as pd
import polars as pl
from aioch import Client

logger = getLogger(__name__)


class ClickHouseTable:
    def __init__(self, table_name: str, client: Client, database: str = "default"):
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
            logger.info("Table already exists. Skipping creation.")
        else:
            await self.unsafe_create_table(df)

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

    def __init__(self, host: str, port: int, database: str):
        self.client = Client(host=host, port=port, database=database)

    @classmethod
    def get_instance(cls, *args, **kwargs) -> Self:
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def get_table(self, table_name: str) -> ClickHouseTable:
        if table_name not in self._TABLES:
            self._TABLES[table_name] = ClickHouseTable(table_name, self.client)
        return self._TABLES[table_name]

    async def drop_all_tables(self):
        for table_name in self._TABLES:
            await self._TABLES[table_name].drop_table()
        self._TABLES = {}
