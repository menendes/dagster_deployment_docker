from dagster import op, graph
from dagster_aws.s3 import S3Resource
import pandas as pd

from pandas import DataFrame

from pipeline_y.resource.sql_alchemy import SqlAlchemyClientResource


@op
def read_data(context, postgres_io_manager: SqlAlchemyClientResource):
    context.log.info('Read data from database')
    db_engine = postgres_io_manager.create_engine()
    sql_query = "SELECT * FROM engineers"
    data_from_db = pd.read_sql_query(sql_query, db_engine)
    return data_from_db


@op
def transform_data(context, pdf: DataFrame):
    context.log.info("Start transform")
    pdf['Expectation'] = pdf['Salary'] * 2
    filtered_data = pdf[pdf['Expectation'] > 75000]
    context.log.info('Transformed data')
    return filtered_data


@op
def save_data_to_s3(context, pdf: DataFrame, s3_io_manager: S3Resource):
    context.log.info('Save data to S3')
    output_file_name = "output_data.csv"
    pdf.to_csv(output_file_name, index=False)
    s3_io_manager.get_client().upload_file(output_file_name, "etl-dagster-data", output_file_name)


@graph
def etl_op_graph():
    save_data_to_s3(transform_data(read_data()))
