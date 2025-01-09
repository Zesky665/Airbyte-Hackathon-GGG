from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers import requests
from datetime import datetime, timedelta
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)


@dlt.source(name="fingrid")
def fingrid_source(api_key: Optional[str] = dlt.secrets.value) -> Any:
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    client = RESTClient(
        base_url="https://data.fingrid.fi/api",
        headers={
            "x-api-key": api_key,
            "Accept": "application/json"
        }
    )

    datasets = [[181, "WindPower"],
                [191, "HydroPower"],
                [188, "NuclearPower"]
                ]
    start_time = (datetime.now() - timedelta(hours=1)).isoformat()
    end_time = (datetime.now()).isoformat()

    def get_resource(dataset):
        params = {
            "start_time": start_time,
            "end_time": end_time
        }
        endpoint = f"/datasets/{dataset}/data"
        response = client.get(endpoint, params=params)
        print(response.json()['data'])
        yield response.json()['data']

    for dataset in datasets:
        yield dlt.resource(get_resource(dataset[0]), name=dataset[1], 
                                        primary_key=("startTime", "value"), 
                                        write_disposition="replace")


def load_fingrid() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_fingrid",
        destination='motherduck',
        dataset_name="rest_api_data",
    )

    load_info = pipeline.run(fingrid_source(dlt.secrets["api_secret_key"]))
    print(load_info)  # noqa: T201

if __name__ == "__main__":
    load_fingrid()
