import requests
import pandas as pd
from datetime import datetime, timedelta
from dagster import (
    asset, 
    AssetExecutionContext, 
    MetadataValue,
    AssetOut

)
import os
import json
from dagster_duckdb import DuckDBResource

@asset(
        # outs = {'last_run_date': AssetOut()}
        )
def fetch_daily_data_from_api(context:AssetExecutionContext) -> str:
    '''
    Get daily data starting from 2022-01-01 to make batch prediction
    API: https://www.energidataservice.dk/guides/api-guides
    '''
    last_run_date_str = context.metadata.get("last_run_date", "2023-01-01")
    last_run_date = datetime.strptime(last_run_date_str, "%Y-%m-%d")

    start_date = last_run_date + timedelta(days=1)
    end_date = start_date   # take data of 1 day
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

    # Store the current end date for the next run
    context.metadata["last_run_date"] = end_date.strftime("%Y-%m-%d")

    return last_run_date_str

@asset
def save_daily_data_duckdb(
    # duckdb: DuckDBResource, 
                           fetch_daily_data_from_api) -> None:
    last_run_date = fetch_daily_data_from_api
    daily_df = pd.read_json(f"./daily_data/{last_run_date}.json")
    # with duckdb.get_connection() as conn:
    #     conn.execute(f"CREATE TABLE daily.{last_run_date}_data AS SELECT HourDK, PriceArea, ConsumerType_DE35, TotalCon FROM daily_df")
    
@asset
def daily_dataset(
    # duckdb: DuckDBResource, 
    fetch_daily_data_from_api) -> pd.DataFrame:
    last_run_date = fetch_daily_data_from_api
    # with duckdb.get_connection() as conn:
    #     return conn.execute(f"SELECT * from daily.{last_run_date}_data").fetch_df()
