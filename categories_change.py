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
    url = "https://api.coinmarketcap.com/data-api/v3/sector/w/list?sortBy=avg_price_change&sortType=desc"
    response = requests.request("GET", url)
    if (response.status_code != 200):
        raise Exception("Error while query DEX data on CMC")

    return response.json()


@task
def transformCMCData(data):
    return data["data"]


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def loadPostgres(data):
    # Load data to posgres
    url = "https://hasura.n8n.cuthanh.com/api/rest/cmc_categories"
    body = {
        "input": {
            "timestamp": time.time(),
            "data": data
        }
    }

    response = requests.request(
        "POST", url, headers={"x-hasura-admin-secret": "myadminsecretkey", "Content-Type": "'application/json"}, json=body)

    if (response.status_code != 200):
        raise Exception("Error while load data")
    return


with Flow("Crawl-CATEGORIES_CHANGE", schedule=schedule) as flow:
    data = extractCMCTopDEX()
    tranformData = transformCMCData(data)
    loadPostgres(tranformData)

# flow.storage = GitHub(
#     repo="thanhlmm/cmc_data",
#     path="/top_dex.py",
#     ref="main")

flow.run()
# flow.register(project_name="cmc", labels=['n8n.cuthanh.com'])
