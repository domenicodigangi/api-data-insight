from api_fetcher import DomotzAPIDataFetcher
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

ckh_db = ClickhouseDBTables.get_instance("localhost", 9000, "default")
agents_list_table = ckh_db.get_table("agents_list")
cache = InMemoryCache.get_instance()
api_data_fetcher = DomotzAPIDataFetcher(
    DomotzAPISettings(api_key=api_key, base_url=base_url),
    cache=cache,
)
agents_list = await api_data_fetcher.get("agents_list")
agents_list_table.drop_table()
agents_list_table.create_table(agents_list.data)
agents_list_table.insert_data(agents_list.data)
agents_list_table.get_data()


# for all agents make all other nested calls
