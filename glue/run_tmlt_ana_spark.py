"""Using Tumult Analytics by Tumult Labs to execute differentially private queries"""

import sys
import time

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from tmlt.analytics.privacy_budget import PureDPBudget
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.session import Session


# function to create a tumult analytics session with a DataFrame
def _create_tmlt_analytics_session(source_id, df):
    return Session.from_dataframe(
        privacy_budget=PureDPBudget(epsilon=float('inf')),
        source_id=source_id,
        dataframe=df
    )


def run_tmlt_analytics_query(query, epsilon, s3_file_path, column_name, source_id):
    """
    """
    bucket_name, object_key = s3_file_path.replace("s3://", "").split("/", 1)

    spark = SparkSession.builder\
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")\
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")\
        .config("spark.sql.warehouse.dir", f"s3://{bucket_name}/glue/tumult_ana/")\
        .getOrCreate()

    #------------#
    # Dataset    #
    #------------#

    spark_df = spark.read.csv(
        f"s3a://{bucket_name}/{object_key}", header=True, inferSchema=True)
    num_rows = spark_df.count()
    print("Total number of rows in dataset: ", num_rows)

    # session builder for tumult analytics
    session = _create_tmlt_analytics_session(
        source_id, spark_df)

    #----------------------------------------#
    # Compute differentially private queries #
    #----------------------------------------#
    if query == "COUNT":
        begin_time = time.time()
        query_build = QueryBuilder(source_id).count()
    else:
        min_value = spark_df.agg(
            {column_name: "min"}).first()[0]
        max_value = spark_df.agg(
            {column_name: "max"}).first()[0]

        if query == "MEAN":
            begin_time = time.time()
            query_build = QueryBuilder(source_id).average(
                column_name, low=min_value, high=max_value)
        elif query == "SUM":
            begin_time = time.time()
            query_build = QueryBuilder(source_id).sum(
                column_name, low=min_value, high=max_value)
        elif query == "VARIANCE":
            begin_time = time.time()
            query_build = QueryBuilder(source_id).variance(
                column_name, low=min_value, high=max_value)

    # compute
    private_value = session.evaluate(
        query_build,
        privacy_budget=PureDPBudget(epsilon=epsilon)
    ).first()[0]

    # compute execution time
    eps_time_used = time.time() - begin_time

    print("Dataset query result: ", private_value)
    print("Time used: ", eps_time_used)

    spark.stop()


if __name__ == "__main__":

    #----------------#
    # Configurations #
    #----------------#

    args = getResolvedOptions(sys.argv,
                              ['column_name',
                               'epsilon',
                               's3_file_path',
                               'query',
                               'source_id'
                               ])

    # retrieve parameters
    column_name = args['column_name']
    epsilon = int(args['epsilon'])
    s3_file_path = args['s3_file_path']
    query = args['query']  # {MEAN, VARIANCE, COUNT, SUM}
    source_id = args['source_id']  # tumult analytics source_id

    # path to the data in the S3
    s3_dataset_path = s3_file_path

    print("Source ID: ", source_id)
    print("Query: ", query)
    print("Epsilon Values: ", epsilon)
    print("S3 dataset file path: ", s3_file_path)
    print("Column of interest on dataset: ", column_name)

    run_tmlt_analytics_query(
        query, epsilon, s3_file_path, column_name, source_id)
