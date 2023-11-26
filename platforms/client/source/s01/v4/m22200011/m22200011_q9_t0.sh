#!/bin/bash

# Install dependency for pymysql
pip install pymysql

# Install dependency for pymongo
pip install pymongo

# Install dependency for pandas
pip install pandas

# Install dependency for direct_redis
pip install direct_redis

# The below commands are necessary if the Pandas 'read_msgpack' is not directly supported.
# Pandas removed msgpack support in >=1.0 versions, install an older version or alternative package if needed
pip install pandas==0.25.3

# Or if an alternative is preferred (e.g., use orjson for msgpack functionalities):
# pip install orjson
