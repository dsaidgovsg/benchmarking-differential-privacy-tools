"""Using Tumult Analytics by Tumult Labs to execute differentially private queries"""

import getopt
import sys
import time

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

    spark = SparkSession.builder.getOrCreate()

    #------------#
    # Dataset    #
    #------------#

    spark_df = spark.read.csv(
        f"s3://{bucket_name}/{object_key}", header=True, inferSchema=True)
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
    print("Time taken (s): ", eps_time_used)

    spark.stop()


if __name__ == "__main__":

    #----------------#
    # Configurations #
    #----------------#

    argv = sys.argv[1:]

    try:
        opts, args = getopt.getopt(
            argv, "c:e:i:f:q:", ["column =", "epsilon =", "id =", "file =", "query ="])
    except:
        print("Invalid inputs")
        exit(1)

    # retrieve parameters
    for opt, arg in opts:
        if opt in ["-c", "--column"]:
            column_name = arg
        elif opt in ["-e", "--epsilon"]:
            epsilon = float(arg)
        elif opt in ["-i", "--id"]:
            source_id = arg
        elif opt in ["-f", "--file"]:
            s3_file_path = arg
        elif opt in ["-q", "--query"]:
            query = arg  # {MEAN, VARIANCE, COUNT, SUM}

    print("Source ID: ", source_id)
    print("Query: ", query)
    print("Epsilon Values: ", epsilon)
    print("S3 dataset file path: ", s3_file_path)
    print("Column of interest on dataset: ", column_name)

    run_tmlt_analytics_query(
        query, epsilon, s3_file_path, column_name, source_id)
