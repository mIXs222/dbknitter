#!/bin/bash

# Ensure package lists are up to date before starting
apt-get update

# Installing Python and Pip
apt-get install -y python3 python3-pip

# Installing MongoDB driver for Python
pip3 install pymongo

# Installing pandas for data manipulation in Python
pip3 install pandas

# Installing DirectRedis for connecting to Redis
pip install direct_redis
