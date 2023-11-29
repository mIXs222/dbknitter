#!/bin/bash

# Create a virtual environment (optional)
python3 -m venv query_env
source query_env/bin/activate

# Install the required packages
pip install pymysql pandas redis==4.3.3 direct_redis
