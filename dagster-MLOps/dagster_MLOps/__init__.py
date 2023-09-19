from dagster import Definitions, load_assets_from_package_module

from dagster_MLOps.assets import training_pipeline

model_training_assets = load_assets_from_package_module(training_pipeline, group_name='training_pipeline')
#batch_prediction_assets = load_assets_from_package_module(batch_prediction_pipeline, group_name='batch_prediction_pipeline')

defs = Definitions(
    assets=model_training_assets,
)
