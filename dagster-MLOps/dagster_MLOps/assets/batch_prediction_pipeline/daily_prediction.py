import pandas as pd
import numpy as np
from dagster import (
    asset, 
    AssetExecutionContext, 
    MetadataValue
)
from dagster_duckdb import DuckDBResource

''' this asset takes predicted consumption rate and validate it against real values
    - Prediction takes place before the start of the date (at 9pm previous day etc)
    - Real values of a date is recorded after the day finishes (at 2am next day etc)

    So the approach is to run validation of the previous day's results on the next day
    Example: 
    - On 31-12-2022 we make a prediction for 1-1-2023
    - On 2-1-2023 we crawl real consumption value of 1-1-2023 (24h)
    - On 2-1-2023 we validate the predicted values with the real values
'''

'''
Run everyday prediction based on stored features and registered model
'''

## need to change this to self populate daily dataset
@asset
def feature_engineered_daily_data(tbd) -> pd.DataFrame:
    df_feature_engineered = tbd

    # Change hour column to datetime dtype
    df_feature_engineered['HourDK'] = pd.to_datetime(df_feature_engineered['HourDK'], errors = 'coerce')
    df_feature_engineered['HourDK_year'] = df_feature_engineered['HourDK'].dt.year.astype(np.int64)
    df_feature_engineered['HourDK_month'] = df_feature_engineered['HourDK'].dt.month.astype(np.int64)
    df_feature_engineered['HourDK_day'] = df_feature_engineered['HourDK'].dt.day.astype(np.int64)
    df_feature_engineered['HourDK_hour'] = df_feature_engineered['HourDK'].dt.hour.astype(np.int64)
    df_feature_engineered['HourDK_weekofyear'] = df_feature_engineered['HourDK'].dt.isocalendar().week.astype(np.int64)
    df_feature_engineered['HourDK_dayofweek'] = df_feature_engineered['HourDK'].dt.dayofweek.astype(np.int64)
    df_feature_engineered['ConsumerType_DE35'] = df_feature_engineered['ConsumerType_DE35'].astype(str)
    df_feature_engineered = df_feature_engineered.drop(columns=['HourDK'])

    return df_feature_engineered

@asset
def transformed_daily_data(feature_engineered_daily_data, scaler, encoder) -> pd.DataFrame:
    X = feature_engineered_daily_data.drop(columns=['TotalCon'])
    inference_X = X.copy()

    scaler = scaler
    encoder = encoder

    numeric_features = ['HourDK_year','HourDK_month','HourDK_day','HourDK_hour','HourDK_weekofyear','HourDK_dayofweek']
    categorical_features = ['PriceArea', 'ConsumerType_DE35']

    inference_X[numeric_features] = scaler.transform(X[numeric_features])

    categorical_encoded = encoder.transform(X[categorical_features]).toarray()
    categorical_encoded_df = pd.DataFrame(categorical_encoded, columns=encoder.get_feature_names_out(categorical_features))
    inference_X.drop(categorical_features, axis=1, inplace=True)
    inference_X = pd.concat([inference_X, categorical_encoded_df], axis=1)

    return inference_X

# use registered model
@asset
def daily_new_prediction(transformed_daily_data, tbd):
    inference_X = transformed_daily_data
    model = tbd

    return model.predict(inference_X)

@asset
def save_daily_prediction(duckdb: DuckDBResource, transformed_daily_data, daily_new_prediction, last_run_date) -> None:
    inference_X = transformed_daily_data
    last_run_date = last_run_date

    predicted_y = pd.DataFrame(daily_new_prediction)
    new_predictions_df = pd.concat([inference_X, predicted_y], axis=1)

    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE TABLE dailypred.{last_run_date}_predictions AS SELECT HourDK, PriceArea, ConsumerType_DE35, TotalCon FROM new_predictions_df")
