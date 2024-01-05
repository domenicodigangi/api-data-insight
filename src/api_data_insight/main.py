import asyncio

from api_fetcher import DomotzAPIDataFetcher
from api_fetcher.cache.memory import InMemoryCache
from api_fetcher.settings import BASE_URLS, DomotzAPISettings
from dotenv import dotenv_values

from api_data_insight.data_persistency.clickhouse import ClickhouseDBTables


async def fetch_all(env: dict[str, str]):
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

    agents_tab, agents_df = await ckh_db.table_from_api(api_data_fetcher, "agents_list")
    list_devices_tab, list_devices_df = await ckh_db.table_from_api(
        api_data_fetcher, "list_devices", path_params={"agent_id": agent_id}
    )
    for agent_id in agents_df["id"]:
        await ckh_db.table_from_api(
            api_data_fetcher,
            "list_device_variables",
            path_params={"agent_id": agent_id},
        )

    await ckh_db.get_table("list_device_variables").get_data()
    devices_df = await ckh_db.get_table("list_devices").get_data()

    for agent_id in agents_df["id"]:
        for device_id in devices_df["id"]:
            await ckh_db.table_from_api(
                api_data_fetcher,
                "list_device_variables",
                path_params={"agent_id": agent_id},
            )


if __name__ == "__main__":
    env = dotenv_values("/workspaces/public-api-insight/.env")
    asyncio.run(fetch_all(env))
