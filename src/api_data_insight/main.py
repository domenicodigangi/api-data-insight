from api_fetcher.cache.memory import InMemoryCache
from api_fetcher.settings import BASE_URLS, DomotzAPISettings
from dotenv import dotenv_values

from api_data_insight.data_persistency.clickhouse import (
    ClickhouseDBTables,
    ClickHouseTable,
)

env = dotenv_values("/workspaces/public-api-insight/.env")


table_name = "agents_list"
api_key = env["API_KEY_EU"]
base_url = BASE_URLS["EU"]

ClickhouseDBTables.get_instance
clickhouse_table = ClickHouseTable(table_name)
cache = InMemoryCache.get_instance()
api_data_fetcher = DomotzAPICaller(
    DomotzAPISettings(api_key=api_key, base_url=base_url),
    cache=cache,
)
df_data = await api_data_fetcher.get("agents_list")
clickhouse_table.create_table(df_data)
clickhouse_table.insert_data(df_data)
