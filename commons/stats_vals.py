TUMULT_ANALYTICS = "tmlt_ana"
OPENDP = "opendp"
DIFFPRIVLIB = "diffprivlib"
PIPELINEDP_LOCAL = "pipelinedp_local"
PIPELINEDP_SPARK = "pipelinedp_spark"

# queries to experiment
MEAN = "mean"
COUNT = "count"
SUM = "sum"
VARIANCE = "variance"

VALID_QUERIES = [COUNT, SUM, MEAN, VARIANCE]

VALID_SIZES = ["1k", "10k", "100k", "1M", "10M", "100M", "1B"]
SIZES_DICT = {
    "1k": 1000,
    "10k": 10_000,
    "100k": 100_000,
    "1M": 1000_000,
    "10M": 10_000_000,
    "100M": 100_000_000,
    "1B": 1000_000_000
}

BASE_PATH = "/Users/anshusingh/DPPCC/whitespace/differential_privacy/"

EPSILON_VALUES = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
                  0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
                  1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# for synthetic datasets the column name is fixed
DEFAULT_COLUMN_NAME = "values"

S3_DATA_BUCKET = "dp-experiments-data-public"
S3_OUTPUT_BUCKET = "dp-experiments-outputs"
