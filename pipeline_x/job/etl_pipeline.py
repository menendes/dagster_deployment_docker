import time

from dagster import op, graph
from dagster_aws.s3 import S3Resource
import pandas as pd

from pandas import DataFrame

from pipeline_x.resource.sql_alchemy import SqlAlchemyClientResource


@op
def read_s3_data(context, s3_io_manager: S3Resource):
    # Read data from a CSV file
    time.sleep(90)
    s3_io_manager.get_client().get_object(Bucket="etl-dagster-data", Key="input_data.csv")
    context.log.info('Client connection success')
    with open("data/input_data.csv", 'wb') as f:
        s3_io_manager.get_client().download_fileobj("etl-dagster-data", "input_data.csv",
                                                    f)  # TODO just read data dont download it
    data = pd.read_csv('data/input_data.csv')
    return data


@op
def transform_data(context, pdf: DataFrame):
    context.log.info("Start transform")
    filtered_data = pdf[pdf['Salary'] > 45000]
    context.log.info('Transformed data')
    return filtered_data


@op
def load_data(context, transformed_data, postgres_io_manager: SqlAlchemyClientResource):
    # Load data into the database
    transformed_data.to_sql('engineers', postgres_io_manager.create_engine(), if_exists='replace', index=False)
    context.log.info('Loaded transformed data into the database')


@graph
def etl_op_graph():
    load_data(transform_data(read_s3_data()))

