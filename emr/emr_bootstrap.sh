#!/bin/bash

sudo python3 -m ensurepip --upgrade

### Tumult Analytics
sudo python3 -m pip install tmlt.analytics==0.6.1
# pyspark is explicitly uninstalled so that it doesn’t override the version installed as part of EMR, which has files necessary for interacting with AWS.
sudo python3 -m pip uninstall -y pyspark 

### pipelineDP and other libraries used in scripts
sudo python3 -m pip install \
    botocore \
    boto3 \
    psutil \
    tqdm \
    fsspec \
    s3fs \
    pandas \
    pipeline_dp
