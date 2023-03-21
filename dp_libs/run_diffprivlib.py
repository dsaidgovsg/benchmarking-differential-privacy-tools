"""Using IBM's Diffprivlib library to execute differentially private queries"""

import os
import time
import psutil
import pandas as pd
from tqdm import tqdm

from diffprivlib.tools import count_nonzero, sum, mean, var

from commons.stats_vals import DIFFPRIVLIB, \
    DEFAULT_COLUMN_NAME, \
    MEAN, \
    VARIANCE, \
    COUNT, \
    SUM

from commons.utils import save_synthetic_data_query_ouput


def run_query(query, epsilon_values, per_epsilon_iterations, data_path, output_folder):
    """
    """

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

        # budget_acc = BudgetAccountant()

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
                if query == COUNT:
                    begin_time = time.time()
                    private_value = count_nonzero(data, epsilon=epsilon)
                else:
                    min_value = data.min()
                    max_value = data.max()

                    if query == MEAN:
                        begin_time = time.time()
                        private_value = mean(
                            data, epsilon=epsilon, bounds=(min_value, max_value))
                    elif query == SUM:
                        begin_time = time.time()
                        private_value = sum(
                            data, epsilon=epsilon, bounds=(min_value, max_value))
                    elif query == VARIANCE:
                        begin_time = time.time()
                        private_value = var(
                            data, epsilon=epsilon, bounds=(min_value, max_value))

                # compute execution time
                eps_time_used.append(time.time() - begin_time)

                # compute memory usage
                eps_memory_used.append(process.memory_info().rss)  # in bytes

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

            save_synthetic_data_query_ouput(DIFFPRIVLIB, query, epsilon, filename, eps_errors,
                                            eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)


# if __name__ == "__main__":

#     #----------------#
#     # Configurations #
#     #----------------#
#     evaluate_query = SUM
#     evaluate_size = 1000

#     # assert evaluate_query in [COUNT, SUM, MEAN, VARIANCE]
#     # assert evaluate_size in ["1k", "10k", ]

#     # number of iterations to run for each epsilon value
#     per_epsilon_iterations = 100
#     epsilon_values = EPSILON_VALUES

#     # path to the folder containing CSVs of `dataset_size` size
#     dataset_path = BASE_PATH + f"data/synthetic_data/size_{evaluate_size}/"

#     print("Library: ", DIFFPRIVLIB)
#     print("Query: ", evaluate_query)
#     print("Iterations: ", per_epsilon_iterations)
#     print("Dataset size: ", evaluate_size)
#     print("Dataset path: ", dataset_path)
#     print("Epsilon Values: ", epsilon_values)

#     run_diffprivlib_query(evaluate_query, epsilon_values,
#                           per_epsilon_iterations, dataset_path)
