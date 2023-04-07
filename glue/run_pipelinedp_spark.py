"""Using Google's and OpenMinded's PipelineDP library to execute differentially private queries"""

import sys
import time
from enum import Enum

import pipeline_dp
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

#-----------#
# Constants #
#-----------#


class Query(Enum):
    COUNT = "COUNT"
    MEAN = "MEAN"
    SUM = "SUM"
    VARIANCE = "VARIANCE"


def _compute_dp_metric(rows: list,
                       epsilon: float,
                       metric: pipeline_dp.Metrics,
                       backend: pipeline_dp.LocalBackend,
                       min_val: float = None,
                       max_val: float = None):
    budget_accountant = pipeline_dp.NaiveBudgetAccountant(
        total_epsilon=epsilon, total_delta=0)

    dp_engine = pipeline_dp.DPEngine(budget_accountant, backend)

    params = pipeline_dp.AggregateParams(
        noise_kind=pipeline_dp.NoiseKind.LAPLACE,
        budget_weight=epsilon,
        contribution_bounds_already_enforced=True,
        metrics=[metric],
        max_partitions_contributed=1,
        max_contributions_per_partition=1,
        min_value=min_val,
        max_value=max_val)

    data_extractors = pipeline_dp.DataExtractors(
        partition_extractor=lambda _: 1,
        value_extractor=lambda value: value)

    public_partitions = [1]
    dp_result = dp_engine.aggregate(
        rows, params, data_extractors, public_partitions)

    budget_accountant.compute_budgets()

    return dp_result


def get_spark_context():
    """"""
    # run on spark
    #--------------------------------------------------------------------------------------------#
    # Setup Spark
    # Sample: https://github.com/OpenMined/PipelineDP/blob/main/examples/movie_view_ratings/run_on_spark.py
    #--------------------------------------------------------------------------------------------#

    # connect to a spark cluster for distributed calculation
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    return sc


def run_pipelinedp_query(query, epsilon, s3_file_path, column_name):
    """
    """
    sc = get_spark_context()
    backend = pipeline_dp.SparkRDDBackend(sc)

    #------------#
    # Dataset    #
    #------------#
    bucket_name, object_key = s3_file_path.replace("s3://", "").split("/", 1)

    rdd = sc.textFile(
        f"s3://{bucket_name}/{object_key}")  # .mapPartitions(_parse_partition)

    # get the header row and find the index of the target column
    header = rdd.first()
    header_fields = header.split(",")
    target_col_index = header_fields.index(column_name)

    # remove header row from computation
    rdd = rdd.filter(lambda line: line != header)
    rows = rdd.map(lambda row: row.split(",")[target_col_index]).map(float)

    num_rows = rows.count()
    print("Total number of rows in dataset ", num_rows)

    begin_time = time.time()

    #----------------------------------------#
    # Compute differentially private queries #
    #----------------------------------------#
    if query == Query.COUNT.value:
        begin_time = time.time()
        dp_result = _compute_dp_metric(
            rows, epsilon, pipeline_dp.Metrics.COUNT, backend)
    else:
        min_value = rows.min()
        max_value = rows.max()

        if query == Query.MEAN.value:
            begin_time = time.time()
            print("epsilon:", epsilon)
            dp_result = _compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.MEAN,
                                           backend, min_value, max_value)
        elif query == Query.SUM.value:
            begin_time = time.time()
            dp_result = _compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.SUM,
                                           backend, min_value, max_value)
        elif query == Query.VARIANCE.value:
            begin_time = time.time()
            dp_result = _compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.VARIANCE,
                                           backend, min_value, max_value)

    private_value = dp_result.collect()[0][1][0]

    # compute execution time
    eps_time_used = time.time() - begin_time

    print("Dataset query result: ", private_value)
    print("Time taken (s): ", eps_time_used)

    sc.stop()


if __name__ == "__main__":

    #----------------#
    # Configurations #
    #----------------#
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