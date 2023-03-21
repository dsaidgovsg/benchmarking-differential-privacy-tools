"""Common utility functions"""

import os
import time
import pandas as pd
import numpy as np

def strip_end(text: str, suffix: str):
    if suffix and text.endswith(suffix):
        return text[:-len(suffix)]
    return text


def save_synthetic_data_query_ouput(lib_name: str,
                                    query: str,
                                    epsilon: float,
                                    filename: str,
                                    error: float,
                                    relative_errors: list,
                                    scaled_errors: list,
                                    time_used: list,
                                    memory_used: list,
                                    output_folder: str = time.strftime("%m-%d")):
    """
    Function to save the outputs in a CSV file
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

    directory = f"outputs/{output_folder}/{lib_name}/size_{out['dataset_size']}/"

    if not os.path.exists(directory):
        os.makedirs(directory)

    output_path = directory + f"{query}.csv"
    df.to_csv(output_path, mode="a", header=not os.path.exists(
        output_path), index=False)

    print(f"Saved results for epsilon: {epsilon}")

    return out["mean_time_used"]
