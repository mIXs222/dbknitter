#!/bin/bash
# install_dependencies.sh

# Upgrade pip to the latest version
pip install --upgrade pip

# Install pymysql for MySQL connection
pip install pymysql

# Install pandas for data manipulation
pip install pandas

# Install direct_redis for Redis connection
pip install git+https://github.com/aromatt/thejimquisition.git@master#egg=direct_redis
