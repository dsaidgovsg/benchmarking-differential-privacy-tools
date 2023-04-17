"""Using Google's and OpenMinded's PipelineDP library to execute differentially private queries"""

import os
import time
import psutil
import pandas as pd
from tqdm import tqdm

import pipeline_dp

from commons.stats_vals import PIPELINEDP_LOCAL, \
    DEFAULT_COLUMN_NAME, \
    MEAN, \
    VARIANCE, \
    COUNT, \
    SUM
from commons.utils import save_synthetic_data_query_ouput


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
        # privacy_id_extractor=lambda row: row["id"],
        partition_extractor=lambda _: 1,
        value_extractor=lambda row: row[DEFAULT_COLUMN_NAME])

    public_partitions = [1]
    dp_result = dp_engine.aggregate(
        rows, params, data_extractors, public_partitions)

    # dp_result = dp_engine.aggregate(
    #     rows, params, data_extractors, public_partitions, explain_computation_report)

    budget_accountant.compute_budgets()

    # print(explain_computation_report.text())

    return list(dp_result)


def run_query(query: str,
              epsilon_values: list,
              per_epsilon_iterations: int,
              data_path: str,
              output_folder: str):

    backend = pipeline_dp.LocalBackend()

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
        data = df[DEFAULT_COLUMN_NAME]
        num_rows = data.count()

        rows = [index_row[1] for index_row in df.iterrows()]

        #----------#
        # Epsilons #
        #----------#
        print("epsilon_values", epsilon_values)
        for epsilon in epsilon_values:
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
                    dp_result = _compute_dp_metric(
                        rows, epsilon, pipeline_dp.Metrics.COUNT, backend)
                else:
                    # we default the clamping bounds as the min and max values 
                    # for our experimental setting. 
                    # Note: unknown (non-public) min and max values should be 
                    # computed in private manner (with privacy budget spending)
                    # or estimated.  
                    min_value = data.min()
                    max_value = data.max()

                    if query == MEAN:
                        begin_time = time.time()
                        print("epsilon:", epsilon)
                        dp_result = _compute_dp_metric(
                            rows, epsilon, pipeline_dp.Metrics.MEAN, backend, min_value, max_value)
                    elif query == SUM:
                        begin_time = time.time()
                        dp_result = _compute_dp_metric(
                            rows, epsilon, pipeline_dp.Metrics.SUM, backend, min_value, max_value)
                    elif query == VARIANCE:
                        begin_time = time.time()
                        dp_result = _compute_dp_metric(rows, epsilon, pipeline_dp.Metrics.VARIANCE,
                                                       backend, min_value, max_value)

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

            save_synthetic_data_query_ouput(PIPELINEDP_LOCAL, query, epsilon, filename, eps_errors,
                                            eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)
