#!/bin/bash

# Install MySQL driver
pip install pymysql

# Install MongoDB driver
pip install pymongo

# Install Pandas library
pip install pandas

# Simulate the installation command for the direct_redis library
# (Assuming direct_redis.DirectRedis behaves like redis.Redis regarding connection)
# In practice, replace the command below with the actual installation command
# for the direct_redis module once it is made available.
pip install direct_redis

# Run the python code to execute the query
python query_exec.py
