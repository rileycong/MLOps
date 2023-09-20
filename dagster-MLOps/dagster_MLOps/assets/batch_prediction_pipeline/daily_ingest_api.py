import requests
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from dagster import (
    AssetIn,
    asset,
    Output,
    AssetKey,
    AssetExecutionContext, 
    MetadataValue,
)
from dagster_duckdb import DuckDBResource

@asset
def fetch_daily_data_from_api(context:AssetExecutionContext) -> Output[str]:
    '''
    Get daily data starting from 2022-01-01 to make batch prediction
    API: https://www.energidataservice.dk/guides/api-guides
    '''
    # Get the model accuracy from metadata of the previous materilization of this machine learning model
    instance = context.instance
    materialization = instance.get_latest_materialization_event(
        AssetKey(["fetch_daily_data_from_api"])
    )
    if materialization is None:
        last_run_date_str = "2022-12-31"

    else:
        last_run_date_str = materialization.asset_materialization.metadata["last_run_date"].value
    
    last_run_date = datetime.strptime(last_run_date_str, "%Y-%m-%d")

    start_date = last_run_date + timedelta(days=1)
    end_date = start_date + timedelta(days=1)  # the end date is excluded in the API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    base_url = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"

    params = {
        "start": start_date_str,
        "end": end_date_str,
        "columns": "HourDK,PriceArea,ConsumerType_DE35,TotalCon",
        "limit": "0"
    }

    response = requests.get(base_url, params=params)
    data = response.json()['records']
    
    folder_path = "./daily_data"
    os.makedirs(folder_path, exist_ok=True)

    # Store the data in a JSON file within the folder
    file_path = f"{folder_path}/{start_date_str}.json"
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

    context.log.info(f"{start_date_str} data saved to {file_path}")

    last_run_date_str = start_date_str

    return Output(last_run_date_str, metadata={"last_run_date": last_run_date_str})

@asset(ins={"last_run_date": AssetIn("fetch_daily_data_from_api")})
def save_daily_data_duckdb(duckdb: DuckDBResource, last_run_date) -> None:
    last_run_date = last_run_date
    daily_df = pd.read_json(f"./daily_data/{last_run_date}.json")
    with duckdb.get_connection() as conn:
        conn.execute("USE daily")
        conn.execute(f"CREATE TABLE daily.{last_run_date}_data AS SELECT HourDK, PriceArea, ConsumerType_DE35, TotalCon FROM daily_df")
    
@asset(ins={"last_run_date": AssetIn("fetch_daily_data_from_api")})
def daily_dataset(duckdb: DuckDBResource, last_run_date) -> pd.DataFrame:
    last_run_date = fetch_daily_data_from_api
    with duckdb.get_connection() as conn:
        conn.execute("USE daily")
        return conn.execute(f"SELECT * from daily.{last_run_date}_data").fetch_df()
