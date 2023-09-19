import requests
import pandas as pd
from dagster import (
    asset, 
    AssetExecutionContext, 
    MetadataValue
)

@asset(compute_kind='API Ingest')
def training_data_from_api(context:AssetExecutionContext) -> pd.DataFrame:
    
    '''
    Get data from 30-06-2020 to 31-12-2021 to train machine learning model
    API: https://www.energidataservice.dk/guides/api-guides
    '''
    
    base_url = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"

    params = {
        "start": "2020-06-30",
        "end": "2022-01-01",
        "columns": "HourDK,PriceArea,ConsumerType_DE35,TotalCon",
        "limit": "0"
    }

    response = requests.get(base_url, params=params)
    data = response.json()['records']
    
    df = pd.DataFrame(data)
    df['HourDK'] = df['HourDK'] = pd.to_datetime(df['HourDK'])

    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

    return df