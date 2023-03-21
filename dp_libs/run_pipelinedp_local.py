"""Using Google's and OpenMinded's PipelineDP library to execute differentially private queries"""

from commons.utils import save_synthetic_data_query_ouput, update_epsilon_values
import os
import sys
import time
import psutil
import pandas as pd
from tqdm import tqdm

import pipeline_dp
from commons.stats_vals import PIPELINEDP
from commons.stats_vals import BASE_PATH, \
    EPSILON_VALUES, MEAN, VARIANCE, COUNT, SUM

# CHANGES:
# remove private_id
# contribution_bounds_already_enforced
# privacy_id_extractor
# public_partitions = [1]
# pipeline_dp.NaiveBudgetAccountant(total_epsilon=epsilon, total_delta=0
# contribution_bounds_already_enforced=True

#-----------#
# Constants #
#-----------#
LIB_NAME = PIPELINEDP


def compute_dp_metric(rows, epsilon, metric, column_name, backend, min_val=None, max_val=None):
    """"""
    budget_accountant = pipeline_dp.NaiveBudgetAccountant(
        total_epsilon=epsilon, total_delta=0)

    # explain_computation_report = pipeline_dp.ExplainComputationReport()

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
        # privacy_id_extractor=lambda row: row["id"],
        partition_extractor=lambda _: 1,
        value_extractor=lambda row: row[column_name])

    public_partitions = [1]
    dp_result = dp_engine.aggregate(
        rows, params, data_extractors, public_partitions)

    # dp_result = dp_engine.aggregate(
    #     rows, params, data_extractors, public_partitions, explain_computation_report)

    budget_accountant.compute_budgets()

    # print(explain_computation_report.text())

    return list(dp_result)


def run_query(query, epsilon_values, per_epsilon_iterations, data_path, limiting_time_sec, output_folder):
    """
    """

    backend = pipeline_dp.LocalBackend()

    count_exceeded_limit = 0

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

        # library specific setup
        # Reference: https://pipelinedp.io/key-definitions/
        # df['id'] = range(1, len(df) + 1)
        rows = [index_row[1] for index_row in df.iterrows()]

        #----------#
        # Epsilons #
        #----------#
        print("epsilon_values", epsilon_values)
        for epsilon in epsilon_values:
            # print("****----->: ", epsilon)
            eps_time_used = []
            eps_memory_used = []
            eps_errors = []
            eps_relative_errors = []
            eps_scaled_errors = []

            #------------------------#
            # Per epsilon iterations #
            #------------------------#
            for _ in tqdm(range(per_epsilon_iterations)):

                process = psutil.Process(os.getpid())

                #----------------------------------------#
                # Compute differentially private queries #
                #----------------------------------------#
                if query == COUNT:
                    begin_time = time.time()
                    dp_result = compute_dp_metric(
                        rows, epsilon, pipeline_dp.Metrics.COUNT, column_name, backend)
                else:
                    min_value = data.min()
                    max_value = data.max()

                    if query == MEAN:
                        begin_time = time.time()
                        print("epsilon:", epsilon)
                        dp_result = compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.MEAN,
                                                      column_name, backend, min_value, max_value)
                    elif query == SUM:
                        begin_time = time.time()
                        dp_result = compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.SUM,
                                                      column_name, backend, min_value, max_value)
                    elif query == VARIANCE:
                        begin_time = time.time()
                        dp_result = compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.VARIANCE,
                                                      column_name, backend, min_value, max_value)

                # rdd action
                private_value = dp_result[0][1][0]

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

                print("true_value:", true_value)
                print("private_value:", private_value)

                # compute errors
                error = abs(true_value - private_value)

                eps_errors.append(error)
                eps_relative_errors.append(error/abs(true_value))
                eps_scaled_errors.append(error/num_rows)

            mean_time_used = save_synthetic_data_query_ouput(LIB_NAME, query, epsilon, filename, eps_errors,
                                                             eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)

            if mean_time_used >= limiting_time_sec:
                if count_exceeded_limit >= 5:
                    print(
                        f"{LIB_NAME} | {filename}: Terminated process!! Last executed epsilon {epsilon} for {query} query.")
                    sys.exit()
                else:
                    count_exceeded_limit += 1


if __name__ == "__main__":

    #----------------#
    # Configurations #
    #----------------#

    limiting_time_sec = 45

    experimental_query = SUM  # {VARIANCE, MEAN, SUM, COUNT}

    dataset_size = 1000

    # path to the folder containing CSVs of `dataset_size` size
    dataset_path = BASE_PATH + f"datasets/synthetic_data/size_{dataset_size}/"

    # for synthetic datasets the column name is fixed (will change for real-life datasets)
    column_name = "values"

    # number of iterations to run for each epsilon value
    # value should be in [100, 500]
    per_epsilon_iterations = 50  # for the testing purpose low value is set

    epsilon_values = EPSILON_VALUES

    # get the epsilon values to resume with
    # output_file = f"outputs/synthetic/{LIB_NAME.lower()}/size_{dataset_size}/{experimental_query}.csv"
    # if os.path.exists(output_file):
    #     epsilon_values = update_epsilon_values(output_file)

    # test if all the epsilon values have NOT been experimented with
    if epsilon_values != -1:

        print("Library: ", LIB_NAME, " on Local")
        print("Query: ", experimental_query)
        print("Iterations: ", per_epsilon_iterations)
        print("Dataset size: ", dataset_size)
        print("Dataset path: ", dataset_path)
        print("Epsilon Values: ", epsilon_values)

        run_pipelinedp_query(experimental_query, epsilon_values,
                             per_epsilon_iterations, dataset_path, column_name, limiting_time_sec)
