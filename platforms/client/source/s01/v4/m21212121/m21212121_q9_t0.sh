#!/bin/bash

# Bash script (install_dependencies.sh)

# Activate environment or create a new one
# For example, using conda:
# conda create --name query_env python=3.8
# conda activate query_env

# Install pymongo for MongoDB connection
pip install pymongo

# Install pandas for data manipulation
pip install pandas

# Install direct_redis for Redis connection
pip install git+https://github.com/pbs/direct_redis.git

# The user can run the Python script after installing all dependencies by executing:
# python execute_query.py
