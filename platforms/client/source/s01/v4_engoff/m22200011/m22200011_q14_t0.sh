#!/bin/bash

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip to the latest version
pip install --upgrade pip

# Install the dependencies
pip install pymongo pandas redis direct_redis

# Run the python script provided
python3 run_query.py
