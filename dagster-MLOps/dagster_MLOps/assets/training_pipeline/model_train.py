from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import json
from dagster import asset, AssetExecutionContext, MetadataValue

@asset
def randomforest_model(transformed_training_data):
    transformed_X_train, transformed_y_train = transformed_training_data
    rfr = RandomForestRegressor(n_estimators=200, max_depth=20)
    rfr.fit(transformed_X_train, transformed_y_train)
    return rfr

@asset
def gradientboost_model(transformed_training_data):
    transformed_X_train, transformed_y_train = transformed_training_data
    gbr = GradientBoostingRegressor(n_estimators=200, max_depth=4, learning_rate=0.1)
    gbr.fit(transformed_X_train, transformed_y_train)
    return gbr

@asset
def rfr_model_performance(context:AssetExecutionContext, transformed_test_data, randomforest_model):
    transformed_X_test, transformed_y_test = transformed_test_data
    score = randomforest_model.score(transformed_X_test, transformed_y_test)
    params = json.dumps(randomforest_model.get_params())
    
    context.add_output_metadata({
        "model": "random forest regressor",
        "params": MetadataValue.text(params),
        "R squared": MetadataValue.float(score)
    })

    return score

@asset
def gbr_model_performance(context:AssetExecutionContext, transformed_test_data, gradientboost_model):
    transformed_X_test, transformed_y_test = transformed_test_data
    score = gradientboost_model.score(transformed_X_test, transformed_y_test)
    params = json.dumps(gradientboost_model.get_params())

    context.add_output_metadata({
        "model": "gradient boost regressor",
        "params": MetadataValue.text(params),
        "R squared": MetadataValue.float(score)
    })

    return score