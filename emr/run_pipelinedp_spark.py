"""Using Google's and OpenMinded's PipelineDP library to execute differentially private queries"""

import os
import sys
import time
from enum import Enum

import boto3
import pandas as pd
import pipeline_dp
import psutil
import pyspark
from awsglue.utils import getResolvedOptions
from pipeline_dp.aggregate_params import (CountParams, MeanParams, SumParams,
                                          VarianceParams)
from pipeline_dp.private_spark import make_private

#-----------#
# Constants #
#-----------#


class Query(Enum):
    COUNT = "COUNT"
    MEAN = "MEAN"
    SUM = "SUM"
    VARIANCE = "VARIANCE"


def _parse_partition(iterator):
    for idx, line in enumerate(iterator):
        try:
            value = float(line)
            yield {"value": value, "id": idx + 1}
        except:
            pass


def _compute_dp_variance(private_rdd, epsilon, min_val, max_val):
    """"""
    # Calculate the private variance
    return private_rdd.variance(
        VarianceParams(
            budget_weight=epsilon,
            noise_kind=pipeline_dp.NoiseKind.LAPLACE,
            max_partitions_contributed=1,
            max_contributions_per_partition=1,
            min_value=min_val,
            max_value=max_val,
            partition_extractor=lambda _: 1,
            value_extractor=lambda val: val["value"]))


def _compute_dp_mean(private_rdd, epsilon, min_val, max_val):
    """"""
    # Calculate the private mean
    return private_rdd.mean(
        MeanParams(
            budget_weight=epsilon,
            noise_kind=pipeline_dp.NoiseKind.LAPLACE,
            max_partitions_contributed=1,
            max_contributions_per_partition=1,
            min_value=min_val,
            max_value=max_val,
            partition_extractor=lambda _: 1,
            value_extractor=lambda val: val["value"]))


def _compute_dp_count(private_rdd, epsilon):
    """"""
    # Calculate the private count
    return private_rdd.count(
        CountParams(
            budget_weight=epsilon,
            noise_kind=pipeline_dp.NoiseKind.LAPLACE,
            max_partitions_contributed=1,
            max_contributions_per_partition=1,
            partition_extractor=lambda _: 1))


def _compute_dp_sum(private_rdd, epsilon, min_val, max_val):
    """"""
    # Calculate the private sum
    return private_rdd.sum(
        SumParams(
            budget_weight=epsilon,
            noise_kind=pipeline_dp.NoiseKind.LAPLACE,
            max_partitions_contributed=1,
            max_contributions_per_partition=1,
            min_value=min_val,
            max_value=max_val,
            partition_extractor=lambda _: 1,
            value_extractor=lambda val: val["value"]))


def get_spark_context():
    """"""
    # run on spark
    #--------------------------------------------------------------------------------------------#
    # Setup Spark
    # Sample: https://github.com/OpenMined/PipelineDP/blob/main/examples/movie_view_ratings/run_on_spark.py
    #--------------------------------------------------------------------------------------------#

    # connect to a spark cluster for distributed calculation

    master = "yarn"
    conf = pyspark.SparkConf().setMaster(master)
    sc = pyspark.SparkContext(conf=conf)

    return sc


def run_pipelinedp_query(query, epsilon, s3_file_path, column_name):
    """
    """
    sc = get_spark_context()

    #------------#
    # Dataset    #
    #------------#
    client = boto3.client('s3')
    bucket_name, object_key = s3_file_path.replace("s3://", "").split("/", 1)
    response = client.get_object(Bucket=bucket_name, Key=object_key)
    df = pd.read_csv(response.get("Body"))

    data = df[column_name]
    num_rows = data.count()
    print("Total number of rows in dataset: ", num_rows)

    try:
        # load file data into Spark's RDD
        data_rdd = sc.textFile(
            f"s3a://{bucket_name}/{object_key}").mapPartitions(_parse_partition)
    except Exception as e:
        print(e)

    begin_time = time.time()

    budget_accountant = pipeline_dp.NaiveBudgetAccountant(
        total_epsilon=100, total_delta=1e-7)

    # wrap Spark's RDD into its private version
    private_rdd = make_private(
        data_rdd, budget_accountant, lambda val: val["id"])

    process = psutil.Process(os.getpid())

    #----------------------------------------#
    # Compute differentially private queries #
    #----------------------------------------#
    if query == Query.COUNT.value:
        dp_result = _compute_dp_count(
            private_rdd, epsilon)
    else:
        min_value = data.min()
        max_value = data.max()

        if query == Query.MEAN.value:
            dp_result = _compute_dp_mean(
                private_rdd, epsilon, min_value, max_value)
        elif query == Query.SUM.value:
            dp_result = _compute_dp_sum(
                private_rdd, epsilon, min_value, max_value)
        elif query == Query.VARIANCE.value:
            dp_result = _compute_dp_variance(
                private_rdd, epsilon, min_value, max_value)

    budget_accountant.compute_budgets()

    # rdd action
    private_value = dp_result.collect()[0][1]

    # compute execution time
    eps_time_used = time.time() - begin_time

    # compute memory usage
    eps_memory_used = process.memory_info().rss

    print("Raw data query result: ", private_value)
    print("Time used: ", eps_time_used)
    print("Memory used: ", eps_memory_used)

    sc.stop()


if __name__ == "__main__":

    #----------------#
    # Configurations #
    #----------------#
    s3 = boto3.resource('s3')

    args = getResolvedOptions(sys.argv,
                              ['column_name',
                               'epsilon',
                               's3_file_path',
                               'query'
                               ])

    # retrieve parameters
    column_name = args['column_name']
    epsilon = int(args['epsilon'])
    s3_file_path = args['s3_file_path']
    query = args['query']  # {MEAN, VARIANCE, COUNT, SUM}

    # path to the data in the S3
    s3_dataset_path = s3_file_path

    print("Query: ", query)
    print("Epsilon Values: ", epsilon)
    print("S3 dataset file path: ", s3_file_path)
    print("Column of interest on dataset: ", column_name)

    run_pipelinedp_query(query, epsilon, s3_file_path, column_name)
