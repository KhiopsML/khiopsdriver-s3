# Khiops driver for AWS S3

This repository hosts the source code for the Khiops filesystem driver enabling transparent manipulation for data stored in AWS S3 buckets.

## Quickstart

If you just want to start using Khiops with your data located on S3, simply install the driver package next to Khiops.
If you installed Khiops the standard way, the driver package can be installed via conda like so:

    conda install -c conda-forge khiops-driver-s3

Or, if you have used your system package manager, you will have to install the driver by the same method. For debian/ubuntu, you will do this:

    CODENAME=$(lsb_release -cs) && \
    TEMP_DEB="$(mktemp)" && \
    wget -O "$TEMP_DEB" "https://github.com/KhiopsML/khiopsdriver-s3/releases/download/0.0.14/khiops-driver-s3_0.0.14-1-${CODENAME}.amd64.deb" && \
    sudo dpkg -i "$TEMP_DEB && \
    rm -f $TEMP_DEB

or if using Rocky linux, do this:

    sudo yum update -y && sudo yum install wget -y && \
    CENTOS_VERSION=$(rpm -E %{rhel}) && \
    TEMP_RPM="$(mktemp).rpm" && \
    wget -O "$TEMP_RPM" "https://github.com/KhiopsML/khiopsdriver-s3/releases/download/0.0.14/khiops-driver-s3_0.0.14-1.el${CENTOS_VERSION}.x86_64.rpm" && \
    sudo yum install "$TEMP_RPM" -y && \
    rm -f $TEMP_RPM

You can check that the driver is installed propery by running

    khiops -s

You should see an output similar to this:

    Khiops 10.3.2

    Drivers:
        'S3 driver' for URI scheme 's3'
    Environment variables:
        None
    Internal environment variables:
        None

which indicates that the driver was loaded properly and will be used for datafiles following the s3:// pattern.

## Authentication

In order to access the data stored on a S3 bucket, in most cases a valid authentication in required. Generally speaking, the Khiops S3 driver supports the same configuration options as the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html#welcome-versions-v2). More specifically, the Khiops S3 driver supports accepts credentials and configuration options provided either via configuration files or environment variables - please refer to the Amazon documentation for the detailed explanations. This means that once you have valid credentials setup in your environment, Khiops will be using these exactly like your python script or Amazon provided tools.

A typical file-based configuration is composed by the pair of `config` and `credentials` files located in the $HOME/.aws folder.

`config`

    [default]
    region=us-east-1
    endpoint_url = https://my-server.cloudprovider.com

`credentials`

    [default]
    aws_access_key_id = AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

Alternatively you can use pass the configuration options and credentials via environment variables:

    export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
    export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    export AWS_DEFAULT_REGION=us-east-1
    export AWS_ENDPOINT_URL=https://my-server.cloudprovider.com

Voilà! You now have access to your data in S3 buckets!

## Logging

You can log information, warnings, errors and debug traces to a file using the following environment variables (they must both be defined to log anything):
- `S3_DRIVER_LOGLEVEL`: available values are `off`, `critical`, `error`, `warning`, `info`, `debug`, `trace` (they are actually the values of the _spdlog_ logging library)
- `S3_DRIVER_LOGFILE`: path to the log file (which does not need to already exist).

> Tip: you can define `S3_DRIVER_LOGFILE` to be `/dev/stderr` or `/dev/stdout` if you want to log to standard error or standard output, respectively.

## Example usage

## Khiops usage (low level)
```
khiops -b -i s3://mydatabucket/khiops_samples/scenario.kh
```

## Python sample
```python
# Imports
import os
from khiops import core as kh

# Set the file paths
dictionary_file_path = "s3://mydatabucket/khiops_samples/Adult/Adult.kdic"
data_table_path = "s3://mydatabucket/khiops_samples/Adult/Adult.txt"
results_dir = "khiops_output"

# Train the predictor
kh.train_predictor(
    dictionary_file_path,
    "Adult",
    data_table_path,
    "class",
    results_dir,
    max_trees=0,
)
```