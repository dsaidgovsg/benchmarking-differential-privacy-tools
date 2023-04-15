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


def _decide_query(rdd, backend, query, epsilon):
    #----------------------------------------#
    # Compute differentially private queries #
    #----------------------------------------#
    if query == Query.COUNT.value:
        begin_time = time.time()
        dp_result = _compute_dp_metric(
            rdd, epsilon, pipeline_dp.Metrics.COUNT, backend)
    else:
        # we default the clamping bounds as the min and max values 
        # for our experimental setting. 
        # Note: unknown (non-public) min and max values should be 
        # computed in private manner (with privacy budget spending)
        # or estimated. 
        min_value = rdd.min()
        max_value = rdd.max()

        if query == Query.MEAN.value:
            begin_time = time.time()
            dp_result = _compute_dp_metric(rdd, epsilon, pipeline_dp.Metrics.MEAN,
                                           backend, min_value, max_value)
        elif query == Query.SUM.value:
            begin_time = time.time()
            dp_result = _compute_dp_metric(rdd, epsilon, pipeline_dp.Metrics.SUM,
                                           backend, min_value, max_value)
        elif query == Query.VARIANCE.value:
            begin_time = time.time()
            dp_result = _compute_dp_metric(rdd, epsilon, pipeline_dp.Metrics.VARIANCE,
                                           backend, min_value, max_value)

    private_value = dp_result.collect()[0][1][0]
    # compute execution time
    eps_time_used = time.time() - begin_time

    return private_value, eps_time_used


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
    raw_rdd = rdd.map(lambda row: row.split(",")[target_col_index]).map(float)

    print("Total number of rows in dataset: ", raw_rdd.count())
    print("True mean value: ", raw_rdd.mean())

    raw_private_value, raw_eps_time_used = _decide_query(
        raw_rdd, backend, query, epsilon)

    print("Dataset query result: ", raw_private_value)
    print("Time taken (s): ", raw_eps_time_used)

    # dropping a row of in the dataset
    indexed_rdd = raw_rdd.zipWithIndex()  # add an index to each element
    index_to_drop = indexed_rdd.takeSample(
        False, 1)[0][1]
    snipped_rdd = indexed_rdd.filter(lambda x: x[1] != index_to_drop).map(
        lambda x: x[0])

    print("Dropped a row, current total number of rows in snipped dataset: ",
          snipped_rdd.count())
    print("Running differential privacy on snipped dataset...")

    snipped_private_value, snipped_eps_time_used = _decide_query(
        snipped_rdd, backend, query, epsilon)

    print("Snipped dataset query result: ", snipped_private_value)
    print("Time taken (s): ", snipped_eps_time_used)

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
