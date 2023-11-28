#!/bin/bash

# Ensure pip is installed and up to date.
python3 -m pip install --upgrade pip

# Install pymysql and direct_redis for MySQL and Redis connections, and pandas for data manipulation.
pip install pymysql direct_redis pandas
