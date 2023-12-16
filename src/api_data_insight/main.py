import asyncio

import pandas as pd
import polars as pl
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
cache = InMemoryCache.get_instance()
api_data_fetcher = DomotzAPIDataFetcher(
    DomotzAPISettings(api_key=api_key, base_url=base_url),
    cache=cache,
)


await ckh_db.drop_all_tables()

ckh_db._TABLES
ckh_db.get_table("agents_list")


async def table_from_api(table_name: str, path_params: dict = {}):
    api_res = await api_data_fetcher.get(table_name, path_params=path_params)
    ch_table = ckh_db.get_table(table_name)
    await ch_table.create_table(api_res.data)
    await ch_table.insert_data(api_res.data)
    await ch_table.get_data()
    return ch_table, api_res.data


agents_tab, agents_df = await table_from_api("agents_list")

for agent_id in agents_df["id"]:
    await table_from_api("list_devices", path_params={"agent_id": agent_id})

for agent_id in agents_df["id"]:
    await table_from_api("list_device_variables", path_params={"agent_id": agent_id})
    

await ckh_db.get_table("list_device_variables").get_data()
devices_df = await ckh_db.get_table("list_devices").get_data()

for device_id in devices_df["id"]:
    await table_from_api("list_device_variables", path_params={"agent_id": agent_id})


api_data_fetcher.standard_calls

# TODO: add agent id to all tables that use it
