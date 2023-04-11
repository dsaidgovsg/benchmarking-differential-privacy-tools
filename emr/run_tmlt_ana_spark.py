"""Using Tumult Analytics by Tumult Labs to execute differentially private queries"""

import getopt
import sys
import time
from enum import Enum

from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, rand
from tmlt.analytics.privacy_budget import PureDPBudget
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.session import Session


class Query(Enum):
    COUNT = "COUNT"
    MEAN = "MEAN"
    SUM = "SUM"
    VARIANCE = "VARIANCE"

# function to create a tumult analytics session with a DataFrame


def _create_tmlt_analytics_session(source_id, df):
    return Session.from_dataframe(
        privacy_budget=PureDPBudget(epsilon=float('inf')),
        source_id=source_id,
        dataframe=df
    )


def _decide_query(spark_df, session, query, epsilon, column_name):
    #----------------------------------------#
    # Compute differentially private queries #
    #----------------------------------------#
    if query == Query.COUNT.value:
        begin_time = time.time()
        query_build = QueryBuilder(source_id).count()
    else:
        min_value = spark_df.agg(
            {column_name: "min"}).first()[0]
        max_value = spark_df.agg(
            {column_name: "max"}).first()[0]

        if query == Query.MEAN.value:
            begin_time = time.time()
            query_build = QueryBuilder(source_id).average(
                column_name, low=min_value, high=max_value)
        elif query == Query.SUM.value:
            begin_time = time.time()
            query_build = QueryBuilder(source_id).sum(
                column_name, low=min_value, high=max_value)
        elif query == Query.VARIANCE.value:
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

    return private_value, eps_time_used


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

    print("Total number of rows in dataset: ", spark_df.count())
    print("True mean value: ", spark_df.select(
        mean(column_name)).collect()[0][0])

    # session builder for tumult analytics
    session = _create_tmlt_analytics_session(
        source_id, spark_df)

    raw_private_value, raw_eps_time_used = _decide_query(
        spark_df, session, query, epsilon, column_name)

    print("Dataset query result: ", raw_private_value)
    print("Time taken (s): ", raw_eps_time_used)

    # dropping a row of in the dataset
    indexed_rdd = spark_df.rdd.zipWithIndex()  # add an index to each element
    index_to_drop = indexed_rdd.takeSample(
        False, 1)[0][1]
    snipped_df = indexed_rdd.filter(lambda x: x[1] != index_to_drop).map(
        lambda x: x[0]).toDF()

    print("Dropped a row, current total number of rows in snipped dataset: ",
          snipped_df.count())
    print("Running differential privacy on snipped dataset...")

    snipped_private_value, snipped_eps_time_used = _decide_query(
        snipped_df, session, query, epsilon, column_name)

    print("Snipped dataset query result: ", snipped_private_value)
    print("Time taken (s): ", snipped_eps_time_used)

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
