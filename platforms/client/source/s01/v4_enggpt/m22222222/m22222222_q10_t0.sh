#!/bin/bash

# It is assumed that Python3 and pip are already installed on the system.

# Upgrade pip to its latest version.
pip install --upgrade pip

# Install pandas (data manipulation) and direct_redis (DirectRedis access).
pip install pandas
pip install git+https://github.com/karletes/direct_redis.git
