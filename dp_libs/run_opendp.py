"""Using the OpenDP library to execute differentially private queries by Harvard"""

import os
import psutil
import time
import pandas as pd
from tqdm import tqdm

from opendp.typing import *
from opendp.mod import binary_search_param
from opendp.measurements import make_base_discrete_laplace, make_base_laplace
from opendp.transformations import make_count, \
    make_clamp, \
    make_bounded_resize, \
    make_bounded_sum, \
    make_sized_bounded_mean, \
    make_sized_bounded_variance

from commons.stats_vals import OPENDP
from commons.stats_vals import BASE_PATH, EPSILON_VALUES, MEAN, VARIANCE, COUNT, SUM
from commons.utils import save_synthetic_data_query_ouput, update_epsilon_values


from opendp.mod import enable_features
enable_features('contrib')
enable_features("floating-point")

#-----------#
# Constants #
#-----------#
LIB_NAME = OPENDP


def run_query(query, epsilon_values, per_epsilon_iterations, data_path, output_folder):
    """"""

    #------------#
    # DATASETS   #
    #------------#
    for filename in os.listdir(data_path):

        print("#"*10)
        print("Filename: ", filename)
        print("#"*10)
        if not filename.endswith(".csv"):
            continue

        df = pd.read_csv(data_path + filename)
        data = df[column_name]
        data_list = data.tolist()

        num_rows = data.count()

        # setup specific to the library
        # the greatest number of records that any one individual can influence in the dataset
        max_influence = 1

        #----------#
        # Epsilons #
        #----------#
        for epsilon in epsilon_values:

            # metrics for `per_epsilon_iterations`
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

                    compute_count = make_count(TIA=float)

                    discovered_scale = binary_search_param(
                        lambda s: compute_count >> make_base_discrete_laplace(
                            scale=s),
                        d_in=max_influence,
                        d_out=float(epsilon))

                    computation_chain = compute_count >> make_base_discrete_laplace(
                        scale=discovered_scale)

                    private_value = computation_chain(data_list)

                else:

                    min_value = data.min()
                    max_value = data.max()

                    if query == MEAN:

                        begin_time = time.time()

                        compute_bounded_mean = (
                            make_clamp(bounds=(min_value, max_value)) >>
                            make_bounded_resize(size=int(num_rows), bounds=(min_value, max_value), constant=0.) >>
                            make_sized_bounded_mean(
                                size=int(num_rows), bounds=(min_value, max_value))
                        )

                        discovered_scale = binary_search_param(
                            lambda s: compute_bounded_mean >> make_base_laplace(
                                scale=s),
                            d_in=max_influence,
                            d_out=float(epsilon))

                        computation_chain = compute_bounded_mean >> make_base_laplace(
                            scale=discovered_scale)

                        private_value = computation_chain(data_list)

                    elif query == SUM:
                        begin_time = time.time()

                        compute_bounded_sum = (
                            make_clamp(bounds=(min_value, max_value)) >>
                            make_bounded_sum(bounds=(min_value, max_value))
                        )

                        discovered_scale = binary_search_param(
                            lambda s: compute_bounded_sum >> make_base_laplace(
                                scale=s),
                            d_in=max_influence,
                            d_out=float(epsilon))

                        computation_chain = compute_bounded_sum >> make_base_laplace(
                            scale=discovered_scale)

                        private_value = computation_chain(data_list)

                    elif query == VARIANCE:
                        begin_time = time.time()

                        compute_bounded_variance = (
                            make_clamp(bounds=(min_value, max_value)) >>
                            make_bounded_resize(size=int(num_rows), bounds=(min_value, max_value), constant=0.) >>
                            make_sized_bounded_variance(
                                size=int(num_rows), bounds=(min_value, max_value))
                        )

                        discovered_scale = binary_search_param(
                            lambda s: compute_bounded_variance >> make_base_laplace(
                                scale=s),
                            d_in=max_influence,
                            d_out=float(epsilon))

                        computation_chain = compute_bounded_variance >> make_base_laplace(
                            scale=discovered_scale)

                        private_value = computation_chain(data_list)

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
    experimental_query = MEAN  # {MEAN, VARIANCE, COUNT, SUM}

    dataset_size = 1000  # {}

    # path to the folder containing CSVs of `dataset_size` size
    dataset_path = BASE_PATH + f"datasets/synthetic_data/size_{dataset_size}/"

    # for synthetic datasets the column name is fixed (will change for real-life datasets)
    column_name = "values"

    # number of iterations to run for each epsilon value
    # value should be in [100, 500]
    per_epsilon_iterations = 100  # for the testing purpose low value is set

    epsilon_values = EPSILON_VALUES

    # get the epsilon values to resume with
    output_file = f"outputs/synthetic/{LIB_NAME.lower()}/size_{dataset_size}/{experimental_query}.csv"
    if os.path.exists(output_file):
        epsilon_values = update_epsilon_values(output_file)

    # test if all the epsilon values have NOT been experimented with
    if epsilon_values != -1:

        print("Library: ", LIB_NAME)
        print("Query: ", experimental_query)
        print("Iterations: ", per_epsilon_iterations)
        print("Dataset size: ", dataset_size)
        print("Dataset path: ", dataset_path)
        print("Epsilon Values: ", epsilon_values)

        run_opendp_query(experimental_query, epsilon_values,
                         per_epsilon_iterations, dataset_path, column_name)
