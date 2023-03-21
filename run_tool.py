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

    parser.add_argument('--tool', type=str, default=DIFFPRIVLIB,
                        help=f'input tool to run \
                        Valid values - {DIFFPRIVLIB, TUMULT_ANALYTICS, OPENDP, PIPELINEDP_LOCAL, PIPELINEDP_SPARK}')
    parser.add_argument('--query', type=str, default=COUNT,
                        help=f'input differential private query to run \
                        Valid values - {COUNT, SUM, MEAN, VARIANCE}')
    parser.add_argument('--size', type=str, default='10k',
                        help='input data size \
                        Valid values - {1k, 10k, 100k}')
    parser.add_argument('--output_folder', type=str, default=time.strftime('%m-%d'),
                        help='folder name to store the output CSV file')

    args = parser.parse_args()

    assert args.tool in [DIFFPRIVLIB, OPENDP,
                         TUMULT_ANALYTICS, PIPELINEDP_LOCAL, PIPELINEDP_SPARK],\
        f"Invalid tool input! Choose among {DIFFPRIVLIB, TUMULT_ANALYTICS, OPENDP, PIPELINEDP_LOCAL, PIPELINEDP_SPARK}"
    assert args.query in [COUNT, SUM, MEAN, VARIANCE], \
        f"Invalid query input! Choose among {COUNT, SUM, MEAN, VARIANCE}"
    assert args.size in ['1k', '10k', '100k'], \
        f"Invalid dataset size input! Choose among 1k, 10k, 100k)"

    #----------------#
    # Configurations #
    #----------------#
    evaluate_tool = args.tool
    evaluate_query = args.query
    evaluate_size = SIZES_DICT[args.size]
    output_folder = args.output_folder

    # number of iterations to run for each epsilon value
    per_epsilon_iterations = 100
    epsilon_values = EPSILON_VALUES
    # path to the folder containing CSVs of `dataset_size` size
    dataset_path = f'data/synthetic_data/size_{evaluate_size}/'

    if evaluate_tool == DIFFPRIVLIB:
        from dp_tools.run_diffprivlib import run_query
    elif evaluate_tool == TUMULT_ANALYTICS:
        from dp_tools.run_tmlt_ana import run_query
    elif evaluate_tool == OPENDP:
        from dp_tools.run_opendp import run_query
    elif evaluate_tool == PIPELINEDP_LOCAL:
        from dp_tools.run_pipelinedp_local import run_query
    elif evaluate_tool == PIPELINEDP_SPARK:
        from dp_tools.run_pipelinedp_spark import run_query
    else:
        raise ValueError('Invalid evaluate_tool')

    print('Library: ', evaluate_tool)
    print('Query: ', evaluate_query)
    print('Dataset Size: ', evaluate_size)
    print('Epsilon Values: ', epsilon_values)
    print('Iterations: ', per_epsilon_iterations)
    print('Dataset Path: ', dataset_path)
    print('Output Path: ', output_folder)

    run_query(evaluate_query, epsilon_values,
              per_epsilon_iterations, dataset_path, output_folder)
