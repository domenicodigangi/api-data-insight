from api_fetcher import DomotzAPICaller
from api_fetcher.settings import DomotzAPISettings, BASE_URLS
from api_fetcher.cache.memory import InMemoryCache
from dotenv import dotenv_values

env = dotenv_values("/workspaces/public-api-insight/.env")
api_caller = DomotzAPICaller(
    DomotzAPISettings(api_key=env["API_KEY_EU"], base_url=BASE_URLS["EU"]), cache = InMemoryCache()
)
api_caller.clear_cache()
agents = await api_caller.get_agents_list()
agent_id = agents.iloc[0, :]["id"]
devices = await api_caller.get_list_devices(agent_id)

task_res = await self.task_helper.define_and_gather_task(
    api_caller.get_device_inventory,
    [(agent_id, row["id"]) for ind, row in devices.iterrows()],
)
device_id = devices.iloc[0, :]["id"]
await api_caller.get_device_inventory(agent_id, device_id)
