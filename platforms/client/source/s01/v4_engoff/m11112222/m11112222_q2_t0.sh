#!/bin/bash
# Bash script to setup the environment

# Create a virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate

# Install pymongo for MongoDB connectivity
pip install pymongo

# Install pandas for data manipulation
pip install pandas

# Install redis and direct_redis for Redis connectivity
pip install redis direct-redis
