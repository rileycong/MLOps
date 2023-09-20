import requests
import json
import pandas as pd
import os
from dagster import (
    asset, 
    AssetExecutionContext, 
    MetadataValue
)
from dagster_duckdb import DuckDBResource

# @asset(compute_kind='API Ingest')
# def training_data_from_api(context:AssetExecutionContext) -> pd.DataFrame:
    
#     '''
#     Get data from 30-06-2020 to 31-12-2021 to train machine learning model
#     API: https://www.energidataservice.dk/guides/api-guides
#     '''
    
#     base_url = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"

#     params = {
#         "start": "2020-06-30",
#         "end": "2022-01-01",
#         "columns": "HourDK,PriceArea,ConsumerType_DE35,TotalCon",
#         "limit": "0"
#     }

#     response = requests.get(base_url, params=params)
#     data = response.json()['records']
    
#     df = pd.DataFrame(data)
#     df['HourDK'] = df['HourDK'] = pd.to_datetime(df['HourDK'])

#     context.add_output_metadata(
#         {
#             "num_records": len(df),
#             "preview": MetadataValue.md(df.head().to_markdown())
#         }
#     )

#     return df

@asset
def fetch_training_data_from_api(context:AssetExecutionContext) -> None:
    '''
    Get data from 30-06-2020 to 31-12-2021 to train machine learning model
    API: https://www.energidataservice.dk/guides/api-guides
    '''
    start_date = "2020-06-30"
    end_date = "2022-01-01"
    base_url = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"

    params = {
        "start": start_date,
        "end": end_date,
        "columns": "HourDK,PriceArea,ConsumerType_DE35,TotalCon",
        "limit": "0"
    }

    response = requests.get(base_url, params=params)
    data = response.json()['records']

    folder_path = "./training_data"
    os.makedirs(folder_path, exist_ok=True)

    # Store the data in a JSON file within the folder
    file_path = f"{folder_path}/{start_date}_{end_date}.json"
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

    context.log.info(f"Data saved to {file_path}")


@asset(deps=[fetch_training_data_from_api])
def save_training_data_duckdb(
    # duckdb: DuckDBResource
    ) -> None:
    training_df = pd.read_json("./training_data/2020-06-30_2022-01-01.json")
    # with duckdb.get_connection() as conn:
    #     conn.execute("CREATE TABLE training.training_dataset AS SELECT HourDK, PriceArea, ConsumerType_DE35, TotalCon FROM training_df")

@asset(deps=[save_training_data_duckdb])
def training_dataset(
    # duckdb: DuckDBResource
    ) -> pd.DataFrame:
    # with duckdb.get_connection() as conn:
    #     return conn.execute("SELECT * from training.training_dataset").fetch_df()
    return 0