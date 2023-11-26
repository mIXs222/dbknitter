#!/bin/bash

# Install MongoDB Python Client
pip install pymongo

# Install Pandas
pip install pandas

# Install direct_redis
pip install git+https://github.com/agilkaya/direct_redis.git

# Run the python script to execute the query
python query.py
