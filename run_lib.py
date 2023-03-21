import time
import argparse
from commons.stats_vals import \
    DIFFPRIVLIB, \
    OPENDP, \
    TUMULT_ANALYTICS, \
    PIPELINEDP_LOCAL, \
    PIPELINEDP_SPARK, \
    EPSILON_VALUES, \
    COUNT, \
    SUM, \
    MEAN, \
    VARIANCE, \
    SIZES_DICT

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--lib', type=str, default=DIFFPRIVLIB,
                        help=f'input library to run \
                        Valid values - {DIFFPRIVLIB, TUMULT_ANALYTICS, OPENDP, PIPELINEDP_LOCAL, PIPELINEDP_SPARK}')
    parser.add_argument('--query', type=str, default=COUNT,
                        help=f'input differential private query to run \
                        Valid values - {COUNT, SUM, MEAN, VARIANCE}')
    parser.add_argument('--size', type=str, default='10k',
                        help='input data size \
                        Valid values - {1k, 10k, 100k}')
    parser.add_argument('--output_folder_name', type=str, default=time.strftime('%m-%d'),
                        help='folder name to store the output CSV file')

    args = parser.parse_args()

    print(args)

    assert args.lib in [DIFFPRIVLIB, OPENDP,
                        TUMULT_ANALYTICS, PIPELINEDP_LOCAL, PIPELINEDP_SPARK]
    assert args.query in [COUNT, SUM, MEAN, VARIANCE]
    assert args.size in ['1k', '10k', '100k']

    #----------------#
    # Configurations #
    #----------------#
    evaluate_lib = DIFFPRIVLIB
    evaluate_query = SUM
    evaluate_size = SIZES_DICT[args.size]
    output_folder = ''

    # number of iterations to run for each epsilon value
    per_epsilon_iterations = 100
    epsilon_values = EPSILON_VALUES
    # path to the folder containing CSVs of `dataset_size` size
    dataset_path = f'data/synthetic_data/size_{evaluate_size}/'

    if evaluate_lib == DIFFPRIVLIB:
        from dp_libs.run_diffprivlib import run_query
    elif evaluate_lib == TUMULT_ANALYTICS:
        from dp_libs.run_tmlt_ana import run_query
    elif evaluate_lib == OPENDP:
        from dp_libs.run_opendp import run_query
    elif evaluate_lib == PIPELINEDP_LOCAL:
        from dp_libs.run_pipelinedp_local import run_query
    elif evaluate_lib == PIPELINEDP_SPARK:
        from dp_libs.run_pipelinedp_local import run_query
    else:
        raise ValueError('Invalid evaluate_lib')

    print('Library: ', evaluate_lib)
    print('Query: ', evaluate_query)
    print('Dataset Size: ', evaluate_size)
    print('Epsilon Values: ', epsilon_values)
    print('Iterations: ', per_epsilon_iterations)
    print('Dataset Path: ', dataset_path)
    print('Output Path: ', output_folder)

    run_query(evaluate_query, epsilon_values,
              per_epsilon_iterations, dataset_path, output_folder)
