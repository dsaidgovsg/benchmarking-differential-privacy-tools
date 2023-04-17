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
from opendp.mod import enable_features

from commons.stats_vals import OPENDP, \
    DEFAULT_COLUMN_NAME, \
    MEAN, \
    VARIANCE, \
    COUNT, \
    SUM
from commons.utils import save_synthetic_data_query_ouput

enable_features('contrib')
enable_features("floating-point")


def run_query(query: str,
              epsilon_values: list,
              per_epsilon_iterations: int,
              data_path: str,
              output_folder: str):

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
        data = df[DEFAULT_COLUMN_NAME]
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
                    # we default the clamping bounds as the min and max values 
                    # for our experimental setting. 
                    # Note: unknown (non-public) min and max values should be 
                    # computed in private manner (with privacy budget spending)
                    # or estimated.  
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

            save_synthetic_data_query_ouput(OPENDP, query, epsilon, filename, eps_errors,
                                            eps_relative_errors, eps_scaled_errors, eps_time_used, eps_memory_used, output_folder)
