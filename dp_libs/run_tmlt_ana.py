"""Using Tumult Analytics by Tumult Labs to execute differentially private queries"""

import os
import psutil
import time
from tqdm import tqdm

from tmlt.analytics.session import Session
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.privacy_budget import PureDPBudget

from pyspark.sql import SparkSession

from commons.utils import save_synthetic_data_query_ouput, update_epsilon_values
from commons.stats_vals import BASE_PATH, EPSILON_VALUES, TUMULT_ANALYTICS, MEAN, VARIANCE, COUNT, SUM

#-----------#
# Constants #
#-----------#
LIB_NAME = TUMULT_ANALYTICS

# function to create a tumult analytics session with a DataFrame
def _create_tmlt_analytics_session(source_id, df):
    return Session.from_dataframe(
        privacy_budget=PureDPBudget(epsilon=float('inf')),
        source_id=source_id,
        dataframe=df
    )


def run_query(query, epsilon_values, per_epsilon_iterations, data_path, column_name, output_folder):
    """"""

    SOURCE_ID = "synthetic_data"

    # master = "local[*]"
    # conf = pyspark.SparkConf().setMaster(master)
    # sc = pyspark.SparkContext(conf=conf)

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

        spark_df = spark.read.csv(data_path + filename, header=True, inferSchema=True)

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
                if query == "COUNT":
                    begin_time = time.time()
                    query_build = QueryBuilder(SOURCE_ID).count()
                else:
                    min_value = spark_df.agg(
                        {column_name: "min"}).first()[0]
                    max_value = spark_df.agg(
                        {column_name: "max"}).first()[0]

                    if query == "MEAN":
                        begin_time = time.time()
                        query_build = QueryBuilder(SOURCE_ID).average(
                            column_name, low=min_value, high=max_value)
                    elif query == "SUM":
                        begin_time = time.time()
                        query_build = QueryBuilder(SOURCE_ID).sum(
                            column_name, low=min_value, high=max_value)
                    elif query == "VARIANCE":
                        begin_time = time.time()
                        query_build = QueryBuilder(SOURCE_ID).variance(
                            column_name, low=min_value, high=max_value)

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
                if query == "MEAN":
                    true_value = spark_df.agg(
                        {column_name: "mean"}).first()[0]
                elif query == "SUM":
                    true_value = spark_df.agg(
                        {column_name: "sum"}).first()[0]
                elif query == "VARIANCE":
                    true_value = spark_df.agg(
                        {column_name: "variance"}).first()[0]
                elif query == "COUNT":
                    true_value = num_rows

                print("true_value:", true_value)
                print("private_value:", private_value)

                # compute errors
                error = abs(true_value - private_value)

                eps_errors.append(error)
                eps_relative_errors.append(error/abs(true_value))
                eps_scaled_errors.append(error/num_rows)

            save_synthetic_data_query_ouput(LIB_NAME, query, epsilon, filename, eps_errors,
                                            eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)


# if __name__ == "__main__":

#     #----------------#
#     # Configurations #
#     #----------------#
#     experimental_query = VARIANCE  # {MEAN, VARIANCE, COUNT, SUM}

#     dataset_size = 10000  # {}

#     # path to the folder containing CSVs of `dataset_size` size
#     dataset_path = BASE_PATH + f"datasets/aws_synthetic_data/size_{dataset_size}/"

#     # for synthetic datasets the column name is fixed (will change for real-life datasets)
#     column_name = "values"

#     # number of iterations to run for each epsilon value
#     # value should be in [100, 500]
#     per_epsilon_iterations = 100  # for the testing purpose low value is set

#     epsilon_values = EPSILON_VALUES

#     # get the epsilon values to resume with
#     # output_file = f"outputs/synthetic/{LIB_NAME.lower()}/size_{dataset_size}/{experimental_query}.csv"
#     # if os.path.exists(output_file):
#     #     epsilon_values = update_epsilon_values(output_file)

#      # test if all the epsilon values have NOT been experimented with
#     if epsilon_values != -1:

#         print("Library: ", LIB_NAME)
#         print("Query: ", experimental_query)
#         print("Iterations: ", per_epsilon_iterations)
#         print("Dataset size: ", dataset_size)
#         print("Dataset path: ", dataset_path)
#         print("Epsilon Values: ", epsilon_values)

#         run_tmlt_analytics_query(experimental_query, epsilon_values,
#                                  per_epsilon_iterations, dataset_path, column_name)
