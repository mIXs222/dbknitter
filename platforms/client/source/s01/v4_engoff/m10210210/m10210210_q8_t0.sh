#!/bin/bash

# Set up a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the required python packages
pip install pymysql pymongo pandas direct_redis

# Run the Python script
python query.py
