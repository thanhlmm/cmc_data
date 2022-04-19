import json
import prefect
from prefect import task, Flow
import requests
import time
from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule

schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=60),
)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def extractVenusData():
    url = "https://api.venus.io/api/governance/venus"
    response = requests.request("GET", url)
    print("Done query: "+url)
    if (response.status_code != 200):
        raise Exception("Error while query Venus data")
    
    responseData = response.json()
    return responseData["data"]


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def loadJitsu(data):
    # Load data to jitsu
    url = "http://n8n.cuthanh.com:8088/api/v1/s2s/event"

    body = {
        "table": "venus",
        "body": data,
    }
    response = requests.request(
            "POST", url, headers={"X-Auth-Token": "s2s.9n9htsciluk9ouvvh97n2q.bz5h9xodjdl25ajx6bise6"}, json=body)
    print(response.json())
    if (response.status_code != 200):
        raise Exception("Error while load data")
    return


with Flow("Crawl-Venus", schedule=schedule) as flow:
    data = extractVenusData()
    loadJitsu(data)

# flow.storage = GitHub(
#     repo="thanhlmm/cmc_data",
#     path="/top_dex.py",
#     ref="main")

flow.run()
# flow.register(project_name="cmc", labels=['n8n.cuthanh.com'])
