"""Using Google's and OpenMinded's PipelineDP library to execute differentially private queries"""

import os
import time
import psutil
import pandas as pd
from tqdm import tqdm

import pyspark
from pipeline_dp.private_spark import make_private
import pipeline_dp
from pipeline_dp.aggregate_params import VarianceParams, MeanParams, SumParams, CountParams

from commons.stats_vals import PIPELINEDP
from commons.stats_vals import BASE_PATH, EPSILON_VALUES, MEAN, VARIANCE, COUNT, SUM
from commons.utils import save_synthetic_data_query_ouput, update_epsilon_values

#-----------#
# Constants #
#-----------#
LIB_NAME = PIPELINEDP

# Framework independent function


def get_spark_context():
    """"""
    # run on spark
    #--------------------------------------------------------------------------------------------#
    # Setup Spark
    # Sample: https://github.com/OpenMined/PipelineDP/blob/main/examples/movie_view_ratings/run_on_spark.py
    #--------------------------------------------------------------------------------------------#

    # connect to a spark cluster for distributed calculation
    master = "local[*]"
    conf = pyspark.SparkConf().setMaster(master)
    sc = pyspark.SparkContext(conf=conf)

    # backend = pipeline_dp.SparkRDDBackend(sc)

    return sc


def _compute_dp_variance(private_rdd, epsilon, min_val, max_val):
    """"""
    # Calculate the private variance
    return private_rdd.variance(
         # out_explain_computaton_report=explain_computation_report, #new 
        VarianceParams(
            # budget_weight=epsilon,
            noise_kind=pipeline_dp.NoiseKind.LAPLACE,
            max_partitions_contributed=1,
            max_contributions_per_partition=1,
            min_value=min_val,
            max_value=max_val,
            partition_extractor=lambda _: 1,
            value_extractor=lambda val: val["value"]),
            public_partitions=[1])


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
    # explain_computation_report = pipeline_dp.ExplainComputationReport()
    # Calculate the private sum
    t = private_rdd.sum(
        SumParams(
            budget_weight=epsilon,
            noise_kind=pipeline_dp.NoiseKind.LAPLACE,
            max_partitions_contributed=1,
            max_contributions_per_partition=1,
            min_value=min_val,
            max_value=max_val,
            partition_extractor=lambda _: 1,
            value_extractor=lambda val: val["value"],
            # contribution_bounds_already_enforced=True #new
        ),
        # out_explain_computaton_report=explain_computation_report, #new
        public_partitions=[1] 
    )

    # print(explain_computation_report.text())

    return t


def _parse_partition(iterator):
    for idx, line in enumerate(iterator):
        try:
            value = float(line)
            yield {"value": value, "id": idx + 1}
        except:
            pass


def run_query(query, epsilon_values, per_epsilon_iterations, data_path, output_folder):
    """
    """
    saprk_context = get_spark_context()

    #------------#
    # Datasets   #
    #------------#
    for filename in os.listdir(data_path):

        print("#"*10)
        print("Filename: ", filename)
        print("#"*10)
        if not filename.endswith(".csv"):
            continue

        df = pd.read_csv(data_path + filename)
        data = df[column_name]
        num_rows = data.count()

        try:
            # load file data into Spark's RDD
            data_rdd = saprk_context.textFile(
                data_path + filename).mapPartitions(_parse_partition)
        except Exception as e:
            print(e)

        # breakpoint()

        #----------#
        # Epsilons #
        #----------#
        for epsilon in epsilon_values:

            print("epsilon: ", epsilon)

            eps_time_used = []
            eps_memory_used = []
            eps_errors = []
            eps_relative_errors = []
            eps_scaled_errors = []

            #------------------------#
            # Per epsilon iterations #
            #------------------------#
            for _ in range(per_epsilon_iterations):

                begin_time = time.time()

                budget_accountant = pipeline_dp.NaiveBudgetAccountant(
                    total_epsilon=epsilon, total_delta=1e-7)

                # wrap Spark's RDD into its private version
                private_rdd = make_private(
                    data_rdd, budget_accountant, lambda val: val["id"])

                process = psutil.Process(os.getpid())

                #----------------------------------------#
                # Compute differentially private queries #
                #----------------------------------------#
                if query == COUNT:
                    dp_result = _compute_dp_count(private_rdd, epsilon)
                else:
                    min_value = data.min()
                    max_value = data.max()

                    if query == MEAN:
                        dp_result = _compute_dp_mean(
                            private_rdd, epsilon, min_value, max_value)
                    elif query == SUM:
                        print("====>", epsilon)
                        dp_result = _compute_dp_sum(
                            private_rdd, epsilon, min_value, max_value)
                    elif query == VARIANCE:
                        dp_result = _compute_dp_variance(
                            private_rdd, epsilon, min_value, max_value)

                budget_accountant.compute_budgets()

                # print(comp.text())

                # rdd action
                private_value = dp_result.collect()[0][1]

                # compute execution time
                eps_time_used.append(time.time() - begin_time)

                # compute memory usage
                eps_memory_used.append(process.memory_info().rss)

                #---------------------#
                # Compute true values #
                #---------------------#
                if query == MEAN:
                    true_value = data.mean()
                elif query == SUM:
                    true_value = data.sum()
                elif query == VARIANCE:
                    true_value = data.var()
                elif query == COUNT:
                    true_value = num_rows

                # print("min_value: ", min_value)
                # print("max_value: ", max_value)
                print("true_value:", true_value)
                print("private_value:", private_value)

                # compute errors
                error = abs(true_value - private_value)

                eps_errors.append(error)
                eps_relative_errors.append(error/abs(true_value))
                eps_scaled_errors.append(error/num_rows)

            save_synthetic_data_query_ouput(LIB_NAME, query, epsilon, filename, eps_errors,
                                            eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)


if __name__ == "__main__":

    #----------------#
    # Configurations #
    #----------------#
    experimental_query = VARIANCE  # {MEAN, VARIANCE, COUNT, SUM}

    dataset_size = 10000  # {}

    # path to the folder containing CSVs of `dataset_size` size
    dataset_path = BASE_PATH + f"datasets/synthetic_data/size_{dataset_size}/"

    # for synthetic datasets the column name is fixed (will change for real-life datasets)
    column_name = "values"

    # number of iterations to run for each epsilon value
    # value should be in [100, 500]
    per_epsilon_iterations = 50  # for the testing purpose low value is set

    epsilon_values = EPSILON_VALUES

    # get the epsilon values to resume with
    # output_file = f"outputs/synthetic/{LIB_NAME.lower()}/size_{dataset_size}/{experimental_query}_spark.csv"
    # if os.path.exists(output_file):
    #     epsilon_values = update_epsilon_values(output_file)

    # test if all the epsilon values have NOT been experimented with
    if epsilon_values != -1:

        print("Library: ", LIB_NAME, " on Spark")
        print("Query: ", experimental_query)
        print("Iterations: ", per_epsilon_iterations)
        print("Dataset size: ", dataset_size)
        print("Dataset path: ", dataset_path)
        print("Epsilon Values: ", epsilon_values)

        run_pipelinedp_query(experimental_query, epsilon_values,
                             per_epsilon_iterations, dataset_path, column_name)
