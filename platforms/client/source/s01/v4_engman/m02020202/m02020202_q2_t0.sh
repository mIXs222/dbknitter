#!/bin/bash
set -e

# Update package list
apt-get update -y

# Install python3 and pip3 if they are not installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas git+https://github.com/yota-py/direct_redis.git
