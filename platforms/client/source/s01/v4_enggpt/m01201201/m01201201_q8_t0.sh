#!/bin/bash

# This script installs all the necessary pip packages to run the `execute_query.py` Python script.

# Update pip to the latest version
pip install --upgrade pip

# Install pymysql to connect to MySQL
pip install pymysql

# Install pymongo to connect to MongoDB
pip install pymongo

# Install direct_redis and pandas - direct_redis is fictional in this example
# If it were a real package, replace this with the correct package.
# As of the knowledge cutoff date, direct_redis does not exist and this is a placeholder.
pip install direct_redis pandas
