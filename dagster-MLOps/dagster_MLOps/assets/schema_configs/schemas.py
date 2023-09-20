from dagster import asset
from dagster_duckdb import DuckDBResource

@asset
def training_schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA training")

@asset(deps=[training_schema])
def save_training_data_duckdb(duckdb: DuckDBResource) -> None:
    training_df = pd.read_json("./training_data/2020-06-30_2022-01-01.json")
    with duckdb.get_connection() as conn:
        conn.execute("USE training")
        conn.execute("CREATE TABLE training.training_dataset AS SELECT HourDK, PriceArea, ConsumerType_DE35, TotalCon FROM training_df")

@asset
def daily_schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA daily")

@asset
def dailypred_schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA dailypred")

