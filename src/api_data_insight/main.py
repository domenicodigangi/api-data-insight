import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict

from api_fetcher import APIDataFetcher
from api_fetcher.cache.memory import InMemoryCache
from api_fetcher.settings import BASE_URLS, DomotzAPISettings
from dotenv import dotenv_values

from api_data_insight.data_persistency.clickhouse import ClickhouseDBTables

logger = logging.getLogger(__name__)


def format_past_datetime(days_history: int) -> str:
    datetime_from = datetime.utcnow() - timedelta(days=days_history)
    return datetime_from.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(
        timespec="seconds"
    )


async def fetch_all(env: Dict[str, str]):
    api_key = env["API_KEY_EU"]
    base_url = BASE_URLS["EU"]
    ckh_db = ClickhouseDBTables.get_instance("localhost", 9000)
    cache = InMemoryCache.get_instance()
    standard_calls = {
        "agents_list": {"path": "/agent", "params": {}},
        "agent": {"path": "/agent/{agent_id}", "params": {}},
        "agent_status_history": {
            "path": "/agent/{agent_id}/history/network/event",
            "params": {},
        },
        "list_devices": {
            "path": "/agent/{agent_id}/device",
            "params": {"show_hidden": True},
        },
        "list_device_variables": {
            "path": "/agent/{agent_id}/device/variable",
            "params": {"page_size": 1000, "has_history": "true"},
        },
        "device_inventory": {
            "path": "/agent/{agent_id}/device/{device_id}/inventory",
            "params": {},
        },
        "history_device_variable": {
            "path": "/agent/{agent_id}/device/{device_id}/variable/{variable_id}/history",
            "params": {"from": format_past_datetime(30)},
        },
    }

    api_data_fetcher = APIDataFetcher(
        DomotzAPISettings(api_key=api_key, base_url=base_url),
        standard_calls=standard_calls,
        cache=cache,
    )
    ckh_db.reset_db()

    agents_tab, agents_df = await ckh_db.table_from_api(api_data_fetcher, "agents_list")

    for agent_id in agents_df["id"]:
        list_devices_tab, list_devices_df = await ckh_db.table_from_api(
            api_data_fetcher, "list_devices", path_params={"agent_id": agent_id}
        )

        await ckh_db.table_from_api(
            api_data_fetcher,
            "list_device_variables",
            path_params={"agent_id": agent_id},
        )

    for agent_id in agents_df["id"]:
        for device_id in devices_df["id"]:
            await ckh_db.table_from_api(
                api_data_fetcher,
                "device_inventory",
                path_params={"agent_id": agent_id, "device_id": device_id},
            )


if __name__ == "__main__":
    env = dotenv_values("/workspaces/public-api-insight/.env")
    asyncio.run(fetch_all(env))


# async def get_all_variables_from_agent(
#     self, agent_id: int
# ) -> Tuple[FormattedDataType, dict[str, dict]]:
#     async def get_history_device_variable(
#         agent_id: int, device_id: int, variable_id: int
#     ) -> CachedDataFormat:
#         return await self.get(
#             "history_device_variable",
#             path_params={
#                 "agent_id": agent_id,
#                 "device_id": device_id,
#                 "variable_id": variable_id,
#             },
#         )

#     variables = await self.get("list_device_variables", {"agent_id": agent_id})
#     variables_history = {}
#     df_variables = variables.data
#     df_variables = df_variables.loc[df_variables["has_history"], :]
#     df_variables["history_hash"] = None
#     df_variables["cache_key"] = None

#     task_res = await self.task_helper.define_and_gather_task(
#         get_history_device_variable,
#         [
#             (agent_id, row["device_id"], row["id"])
#             for ind, row in df_variables.iterrows()
#         ],
#         args_to_ret_inds=[2],
#     )

#     for safe_res in task_res:
#         (
#             success,
#             res,
#             args_retuned,
#             kwargs_retuned,
#         ) = safe_res
#         variable_id = args_retuned[0]
#         var_ind = df_variables["id"] == variable_id
#         if success and res is not None:
#             df_history = res.data
#             variables_history[res.cache_key] = {
#                 "hist": df_history,
#             }
#             df_variables.loc[var_ind, "history_hash"] = hash(df_history.to_json())
#             df_variables.loc[var_ind, "cache_key"] = res.cache_key
#             df_variables.loc[var_ind, "history_len"] = df_history.shape[0]
#         else:
#             df_variables.loc[var_ind, "has_history"] = False

#     df_variables = (
#         df_variables.loc[df_variables["has_history"], :]
#         .groupby(by=["history_hash", "path"])
#         .agg(
#             cache_key=("cache_key", "first"),
#             device_id=("device_id", "first"),
#             id=("id", "first"),
#             replication_count=("id", "count"),
#             metric=("metric", "first"),
#             unit=("unit", "first"),
#         )
#         .reset_index()
#     )
#     variables_history = {
#         k: v
#         for k, v in variables_history.items()
#         if k in df_variables["cache_key"].tolist()
#     }
#     return df_variables, variables_history
