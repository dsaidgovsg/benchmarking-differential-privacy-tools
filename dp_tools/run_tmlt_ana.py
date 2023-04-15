"""Using Tumult Analytics by Tumult Labs to execute differentially private queries"""

import os
import psutil
import time
from tqdm import tqdm

from pyspark.sql import SparkSession

from tmlt.analytics.session import Session
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.privacy_budget import PureDPBudget

from commons.stats_vals import TUMULT_ANALYTICS, \
    DEFAULT_COLUMN_NAME, \
    MEAN, \
    VARIANCE, \
    COUNT, \
    SUM
from commons.utils import save_synthetic_data_query_ouput


def _create_tmlt_analytics_session(source_id: str, df):
    """Function to create a tumult analytics session with a DataFrame"""
    return Session.from_dataframe(
        privacy_budget=PureDPBudget(epsilon=float('inf')),
        source_id=source_id,
        dataframe=df
    )


def run_query(query: str,
              epsilon_values: list,
              per_epsilon_iterations: int,
              data_path: str,
              output_folder: str):

    SOURCE_ID = "synthetic_data"

    # spark set-up
    spark = SparkSession.builder.getOrCreate()

    #------------#
    # Datasets   #
    #------------#
    for filename in os.listdir(data_path):
        print("#"*10)
        print("Filename: ", filename)
        print("#"*10)
        if not filename.endswith(".csv"):
            continue

        spark_df = spark.read.csv(
            data_path + filename, header=True, inferSchema=True)

        num_rows = spark_df.count()

        # session builder for tumult analytics
        session = _create_tmlt_analytics_session(SOURCE_ID, spark_df)

        #----------#
        # Epsilons #
        #----------#
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

                # generate build
                if query == COUNT:
                    begin_time = time.time()
                    query_build = QueryBuilder(SOURCE_ID).count()
                else:
                    # we default the clamping bounds as the min and max values 
                    # for our experimental setting. 
                    # Note: unknown (non-public) min and max values should be 
                    # computed in private manner (with privacy budget spending)
                    # or estimated.  
                    min_value = spark_df.agg(
                        {DEFAULT_COLUMN_NAME: "min"}).first()[0]
                    max_value = spark_df.agg(
                        {DEFAULT_COLUMN_NAME: "max"}).first()[0]

                    if query == MEAN:
                        begin_time = time.time()
                        query_build = QueryBuilder(SOURCE_ID).average(
                            DEFAULT_COLUMN_NAME, low=min_value, high=max_value)
                    elif query == SUM:
                        begin_time = time.time()
                        query_build = QueryBuilder(SOURCE_ID).sum(
                            DEFAULT_COLUMN_NAME, low=min_value, high=max_value)
                    elif query == VARIANCE:
                        begin_time = time.time()
                        query_build = QueryBuilder(SOURCE_ID).variance(
                            DEFAULT_COLUMN_NAME, low=min_value, high=max_value)

                # compute
                private_value = session.evaluate(
                    query_build,
                    privacy_budget=PureDPBudget(epsilon=epsilon)
                ).first()[0]

                # compute execution time
                eps_time_used.append(time.time() - begin_time)

                # compute memory usage
                eps_memory_used.append(process.memory_info().rss)

                #---------------------#
                # Compute true values #
                #---------------------#
                if query == MEAN:
                    true_value = spark_df.agg(
                        {DEFAULT_COLUMN_NAME: "mean"}).first()[0]
                elif query == SUM:
                    true_value = spark_df.agg(
                        {DEFAULT_COLUMN_NAME: "sum"}).first()[0]
                elif query == VARIANCE:
                    true_value = spark_df.agg(
                        {DEFAULT_COLUMN_NAME: "variance"}).first()[0]
                elif query == COUNT:
                    true_value = num_rows

                print("true_value:", true_value)
                print("private_value:", private_value)

                # compute errors
                error = abs(true_value - private_value)

                eps_errors.append(error)
                eps_relative_errors.append(error/abs(true_value))
                eps_scaled_errors.append(error/num_rows)

            save_synthetic_data_query_ouput(TUMULT_ANALYTICS, query, epsilon, filename, eps_errors,
                                            eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)
