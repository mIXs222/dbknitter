#!/bin/bash

# Update package list
apt-get update -y

# Install Python 3 and PIP
apt-get install python3 -y
apt-get install python3-pip -y

# Install pymysql and pandas using PIP
pip3 install pymysql pandas

# Install direct_redis with the necessary dependencies for Redis dataframe loading
pip3 install direct_redis git+https://github.com/pandas-dev/pandas.git # may require additional dependencies
