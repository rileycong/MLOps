import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from dagster import (
    asset, 
    multi_asset, 
    AssetOut,
    AssetExecutionContext,
    MetadataValue
)

@asset
def feature_engineered_data(training_data_from_api) -> pd.DataFrame:
    df = training_data_from_api
    df_feature_engineered = df
    df_feature_engineered['HourDK_year'] = df['HourDK'].dt.year.astype(np.int64)
    df_feature_engineered['HourDK_month'] = df['HourDK'].dt.month.astype(np.int64)
    df_feature_engineered['HourDK_day'] = df['HourDK'].dt.day.astype(np.int64)
    df_feature_engineered['HourDK_hour'] = df['HourDK'].dt.hour.astype(np.int64)
    df_feature_engineered['HourDK_weekofyear'] = df['HourDK'].dt.isocalendar().week.astype(np.int64)
    df_feature_engineered['HourDK_dayofweek'] = df['HourDK'].dt.dayofweek.astype(np.int64)
    df_feature_engineered['ConsumerType_DE35'] = df_feature_engineered['ConsumerType_DE35'].astype(str)
    df_feature_engineered = df_feature_engineered.drop(columns=['HourDK'])

    return df_feature_engineered

@multi_asset(outs={'training_data': AssetOut(), 'test_data': AssetOut()})
def train_test_data(feature_engineered_data):
    df_feature_engineered = feature_engineered_data
    X = df_feature_engineered.drop(columns=['TotalCon'])
    y = df_feature_engineered.TotalCon

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    return (X_train, y_train), (X_test, y_test)

@multi_asset(outs= {'scaler': AssetOut(), 'encoder': AssetOut(), 'transformed_training_data': AssetOut()}
)
def transformed_training_data(context:AssetExecutionContext, training_data):
    X_train, y_train = training_data
    
    scaler = StandardScaler()
    encoder = OneHotEncoder()

    transformed_X_train = X_train.copy()
    transformed_y_train = y_train

    numeric_features = ['HourDK_year','HourDK_month','HourDK_day','HourDK_hour','HourDK_weekofyear','HourDK_dayofweek']
    categorical_features = ['PriceArea', 'ConsumerType_DE35']

    transformed_X_train[numeric_features] = scaler.fit_transform(X_train[numeric_features])

    categorical_encoded = encoder.fit_transform(X_train[categorical_features]).toarray()
    categorical_encoded_df = pd.DataFrame(categorical_encoded, columns=encoder.get_feature_names_out(categorical_features))
    transformed_X_train.drop(categorical_features, axis=1, inplace=True)
    transformed_X_train = pd.concat([transformed_X_train.reset_index(drop=True), categorical_encoded_df], axis=1)
    
    context.add_output_metadata(
        metadata=
        {
            "transformed_X_train": MetadataValue.md(transformed_X_train.head().to_markdown()),
            "transformed_y_train":  MetadataValue.md(transformed_y_train.head().to_markdown())
        },
        output_name="transformed_training_data"
    )

    return scaler, encoder, (transformed_X_train, transformed_y_train)

@asset
def transformed_test_data(context:AssetExecutionContext, test_data, scaler, encoder):
    X_test, y_test = test_data

    transformed_X_test = X_test.copy()
    transformed_y_test = y_test

    numeric_features = ['HourDK_year','HourDK_month','HourDK_day','HourDK_hour','HourDK_weekofyear','HourDK_dayofweek']
    categorical_features = ['PriceArea', 'ConsumerType_DE35']

    transformed_X_test[numeric_features] = scaler.transform(X_test[numeric_features])

    categorical_encoded = encoder.transform(X_test[categorical_features]).toarray()
    categorical_encoded_df = pd.DataFrame(categorical_encoded, columns=encoder.get_feature_names_out(categorical_features))
    transformed_X_test.drop(categorical_features, axis=1, inplace=True)
    transformed_X_test = pd.concat([transformed_X_test.reset_index(drop=True), categorical_encoded_df], axis=1)
    
    context.add_output_metadata(
        metadata=
        {
            "transformed_X_test": MetadataValue.md(transformed_X_test.head().to_markdown()),
            "transformed_y_test":  MetadataValue.md(transformed_y_test.head().to_markdown())
        },
    )

    return transformed_X_test, transformed_y_test