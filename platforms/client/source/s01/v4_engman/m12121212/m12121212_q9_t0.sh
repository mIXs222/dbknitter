#!/bin/bash

# Install pip if not installed
which pip || curl https://bootstrap.pypa.io/get-pip.py | python

# Install required packages
pip install pymongo pandas redis

# Required for direct_redis
pip install git+https://github.com/pandas-dev/pandas.git

# Install DirectRedis (assuming the package is available through the provided instruction)
pip install direct_redis
