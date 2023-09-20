from dagster import (
    Definitions, 
    load_assets_from_package_module,
    AssetSelection, 
    define_asset_job, 
    ScheduleDefinition
    ,schedule
)
from dagster_duckdb import DuckDBResource
from dagster_MLOps.assets import training_pipeline, batch_prediction_pipeline
model_training_assets = load_assets_from_package_module(training_pipeline, group_name='training_pipeline')
batch_prediction_assets = load_assets_from_package_module(batch_prediction_pipeline, group_name='batch_prediction_pipeline')

# scheduled daily prediction
batch_prediction_job = define_asset_job("batch_prediction_job", AssetSelection.groups("batch_prediction_pipeline"))
# daily_prediction_schedule = ScheduleDefinition(job=batch_prediction_job, cron_schedule="0 9 * * *")

@schedule(job=batch_prediction_job, cron_schedule="0 9 * * *")
def batch_prediction_schedule(
):
    pass
    
defs = Definitions(
    assets= [*model_training_assets, *batch_prediction_assets],
    # resources={
    #     # "duckdb": DuckDBResource(
    #     #     database = "./MLOps.duckdb"
    #     # )
    #     "training-duckdb": DuckDBResource(
    #         database = "./db/training.duckdb"
    #     ),
    #     "daily-duckdb": DuckDBResource(
    #         database = "./db/daily.duckdb"
    #     ),
    #     "dailypred-duckdb": DuckDBResource(
    #         database = "./db/dailypred.duckdb"

    #     ),
    # },
    schedules=[batch_prediction_schedule]
)
