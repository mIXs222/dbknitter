#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install python3 and python3-pip
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pandas msgpack-python direct_redis
