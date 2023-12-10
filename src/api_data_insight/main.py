from api_fetcher import DomotzAPIDataFetcher
from api_fetcher.cache.memory import InMemoryCache
from api_fetcher.settings import BASE_URLS, DomotzAPISettings
from dotenv import dotenv_values
import polars as pl
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

table_name = "agents_list"
api_data_fetcher._standard_calls.keys
ch_table = ckh_db.get_table(table_name)
agent_list = await api_data_fetcher.get(table_name)
await ch_table.drop_table()
await ch_table.create_table(api_res.data)
await ch_table.insert_data(api_res.data)
await ch_table.get_data()


api_data_fetcher._standard_calls
agent_id_param_list = [{"agent_id": agent_id} for agent_id in agent_list.data["id"]]

call_name = "list_devices"
ch_table = ckh_db.get_table(call_name)
async for list_devices in api_data_fetcher.get_iterator(call_name, agent_id_param_list):
    await ch_table.safe_insert_data(list_devices.data)
list_devices.data.convert_dtypes().dtypes
list_devices.data["ip_addresses"].iloc[0][0]
len(res)

df = list_devices.data
df["ip_addresses"].astype(list)

pf = pl.from_pandas(df)

def infer_types(df: pd.DataFrame):
    df =df.convert_dtypes()
    for column_name, dtype in df.dtypes.items():
        if dtype == "object":
            if isinstance(df[column_name].iloc[0], list):
                

# for all agents make all other nested calls
