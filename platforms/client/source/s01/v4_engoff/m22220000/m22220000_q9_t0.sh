#!/bin/bash

# Python and pip should be installed

# Ensure pip, setuptools, and wheel are up to date
python3 -m pip install --upgrade pip setuptools wheel

# Install pymysql and pandas library
python3 -m pip install pymysql pandas

# Install direct_redis
python3 -m pip install git+https://github.com/RedisGears/direct_redis.git
