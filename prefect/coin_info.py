import json
import prefect
from prefect import task, Flow
import requests
import time
from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule
from prefect.storage.github import GitHub

schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=60),
)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def extractCoinInfo():
    coin_infos = []
    # TODO: Make the query parallel
    for page in range(1, 100):
        url = f"https://data.messari.io/api/v2/assets?page={page}&limit=500&fields=id,slug,symbol,profile/general/overview/category,profile/general/overview/sector,profile/general/overview/tags,profile/general/overview/tagline"
        response = requests.request("GET", url)
        print("Done query: "+url)
        if (response.status_code != 200):
            print("Error while query Messari data")
            break
        responseData = response.json()

        if (len(responseData["data"]) == 0):
            break

        coin_infos.extend(responseData["data"])

    return coin_infos


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def loadJitsu(data):
    # Load data to jitsu
    # TODO: Make this builk api
    url = "https://jitsu.thanhle.blog/api/v1/events/bulk"

    rawData = ""
    for row in data:
        body = {
            "data": row,
            "table": "coin_info"
        }
        rawData = rawData + json.dumps(body) + "\n"

    files = {'file': ('report.csv', rawData)}
    response = requests.request(
        "POST", url, headers={"X-Auth-Token": "s2s.euvzy95jhm8wnhp33dito.dlvjh8ju8a6gtuar0u6aia"}, files=files)

    print(response.json())
    if (response.status_code != 200):
        raise Exception("Error while load data")
    return


with Flow("Crawl-CATEGORIES_CHANGE", schedule=schedule) as flow:
    data = extractCoinInfo()
    loadJitsu(data)

# flow.storage = GitHub(
#     repo="thanhlmm/cmc_data",
#     path="/top_dex.py",
#     ref="main")

flow.run()
# flow.register(project_name="cmc", labels=['n8n.cuthanh.com'])
