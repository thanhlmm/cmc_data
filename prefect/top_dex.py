import prefect
from prefect import task, Flow
import requests
import time
from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule
from prefect.storage.github import GitHub

schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=30),
)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def extractCMCTopDEX():
    url = "https://api.coinmarketcap.com/data-api/v3/exchange/listing?exType=3&sort=volume_24h"
    response = requests.request("GET", url)
    if (response.status_code != 200):
        raise Exception("Error while query DEX data on CMC")

    return response.json()


@task
def transformCMCData(data):
    return data["data"]["exchanges"]


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def loadJitsu(data):
    # Load data to posgres
    url = "https://jitsu.thanhle.blog/api/v1/s2s/event"
    body = {
        "data": data,
        "table": "top_dex"
    }

    response = requests.request(
        "POST", url, headers={"X-Auth-Token": "s2s.euvzy95jhm8wnhp33dito.dlvjh8ju8a6gtuar0u6aia", "Content-Type": "application/json"}, json=body)

    if (response.status_code != 200):
        raise Exception("Error while load data")
    return


with Flow("Crawl-TOP_DEX", schedule=schedule) as flow:
    data = extractCMCTopDEX()
    tranformData = transformCMCData(data)
    loadJitsu(tranformData)

# flow.storage = GitHub(
#     repo="thanhlmm/cmc_data",
#     path="/top_dex.py",
#     ref="main")

flow.run()
# flow.register(project_name="cmc", labels=['n8n.cuthanh.com'])
