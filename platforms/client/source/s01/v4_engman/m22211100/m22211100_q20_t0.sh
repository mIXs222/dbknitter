#!/bin/bash

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip3 install pymysql pymongo pandas direct_redis

# Execute the python script (assuming the script above is named query_script.py)
python3 query_script.py
