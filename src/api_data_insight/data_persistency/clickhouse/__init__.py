from typing import Self

import pandas as pd
from aioch import Client


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

    async def safe_insert_data(self, df_data: pd.DataFrame):
        if not await self.table_exists():
            await self.create_table(df_data)
        await self.insert_data(df_data)

    async def insert_data(self, df_data: pd.DataFrame):
        if not await self.table_exists():
            raise ValueError("Table does not exist. Please create the table first.")

        df_data = df_data.where(pd.notnull(df_data), None)
        data_tuples = list(df_data.itertuples(index=False, name=None))

        await self.client.execute(
            f"INSERT INTO {self.database}.{self.table_name} VALUES",
            data_tuples,
            types_check=True,
        )

    async def table_exists(self) -> bool:
        result = await self.client.execute(
            f"SELECT 1 FROM system.tables WHERE database = '{self.database}' AND name = '{self.table_name}'"
        )
        return bool(result)

    async def create_table(self, df: pd.DataFrame):
        if await self.table_exists():
            raise ValueError("Table already exists. Please drop the table first.")

        columns = []
        for column_name, dtype in df.dtypes.items():
            clickhouse_type = self._pandas_sql_dtype_mapping.get(str(dtype))
            column_name = str(column_name).replace(".", "_")
            if clickhouse_type:
                columns.append(f"{column_name} Nullable({clickhouse_type})")
            else:
                raise ValueError(
                    f"Unsupported dtype: {dtype} for column: {column_name}"
                )

        create_table_query = f"CREATE TABLE {self.database}.{self.table_name} ({', '.join(columns)}) ENGINE = MergeTree() ORDER BY tuple()"
        await self.client.execute(create_table_query)

    async def drop_table(self):
        if not await self.table_exists():
            raise ValueError("Table does not exist.")

        await self.client.execute(f"DROP TABLE {self.database}.{self.table_name}")

    async def get_data(self) -> pd.DataFrame:
        if not await self.table_exists():
            raise ValueError("Table does not exist.")

        query = f"SELECT * FROM {self.database}.{self.table_name}"
        return pd.DataFrame(await self.client.execute(query))


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
