import os
import time
import pandas as pd
import numpy as np

from commons.stats_vals import EPSILON_VALUES


def update_epsilon_values(output_file):
    """
    """
    out_df = pd.read_csv(output_file)
    last_epsilon = out_df["epsilon"].iloc[-1]

    index = EPSILON_VALUES.index(last_epsilon)

    try:
        return EPSILON_VALUES[index+1:]
    except:
        return -1


def strip_end(text, suffix):
    """
    """
    if suffix and text.endswith(suffix):
        return text[:-len(suffix)]
    return text


def save_synthetic_data_query_ouput(lib_name,
                                    query,
                                    epsilon,
                                    filename,
                                    error,
                                    relative_errors,
                                    scaled_errors,
                                    time_used,
                                    memory_used,
                                    output_folder=time.strftime("%m-%d")):
    """
    """

    rounding_val = 2
    out = {}

    out["epsilon"] = epsilon

    # data_<size>_<scale>_<skew>.csv
    data_feats = filename.split("_")
    out["dataset_size"] = data_feats[1]
    out["dataset_scale"] = data_feats[2]
    out["dataset_skew"] = strip_end(data_feats[3], ".csv")

    out["mean_error"] = round(np.mean(error), rounding_val)
    out["stdev_error"] = round(np.std(error), rounding_val)

    out["mean_relative_error"] = round(np.mean(relative_errors), rounding_val)
    out["stdev_relative_error"] = round(np.std(relative_errors), rounding_val)

    out["mean_scaled_error"] = round(np.mean(scaled_errors), rounding_val)
    out["stdev_scaled_error"] = round(np.std(scaled_errors), rounding_val)

    out["mean_time_used"] = round(np.mean(time_used), rounding_val)
    out["mean_memory_used"] = round(np.mean(memory_used), rounding_val)

    df = pd.DataFrame([out])

    directory = f"outputs/{output_folder}/{lib_name.lower()}/size_{out['dataset_size']}/"

    if not os.path.exists(directory):
        os.makedirs(directory)

    output_path = directory + f"{query.lower()}_REVAMPED_spark.csv"
    df.to_csv(output_path, mode="a", header=not os.path.exists(
        output_path), index=False)

    print(f"Saved results for epsilon: {epsilon}")

    return out["mean_time_used"]
