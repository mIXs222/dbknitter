#!/bin/bash

# install_dependencies.sh

# Update system packages
apt-get update -y

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis, requires git to clone the repository
apt-get install -y git
# git clone https://github.com/RedisJSON/direct_redis.git
# cd direct_redis && python3 setup.py install

# Note: You may need to run the bash script with 'sudo' to install the dependencies system-wide
