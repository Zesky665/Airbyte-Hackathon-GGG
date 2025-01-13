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

    datasets = [["181", "wind_energy_production"],
                ["188", "nuclear_energy_production"],
                ["191", "hydro_energy_production"],
                ["192", "total_energy_production"],
                ["193", "total_energy_consumption"]
               ]
    start_time = (datetime.now() - timedelta(days=3)).isoformat()
    end_time = (datetime.now()).isoformat()

    def get_resource(dataset):
        params = {
            "startTime": start_time,
            "endTime": end_time,
            "pageSize": 10000,
            "oneRowPerTimePeriod": False
        }
        endpoint = f"/datasets/{dataset}/data"
        response = client.get(endpoint, params=params)
        
        data = response.json()['data']
        new_data = [{k: v for k, v in item.items() if k != 'datasetId'} for item in data]
        newer_data = [{k: float(v) if k == 'value' else v for k, v in item.items()} for item in new_data]
        #print(newer_data)
        yield newer_data

    for dataset in datasets:
        yield dlt.resource(get_resource(dataset[0]), name=dataset[1], 
                                        primary_key=("startTime", "endTime"), 
                                        write_disposition="merge")


def load_fingrid() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="incremental_rest_api",
        destination='motherduck',
        dataset_name="raw_dataset"
    )

    #pipeline.run(fingrid_source(), refresh="drop_sources")

    load_info = pipeline.run(fingrid_source(dlt.secrets["api_secret_key"]))

    ##Last pipeline info
    print(load_info)

if __name__ == "__main__":
    load_fingrid()
