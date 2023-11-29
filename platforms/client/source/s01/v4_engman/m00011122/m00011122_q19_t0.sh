#!/bin/bash
# install_dependencies.sh

# Ensure pip is available
sudo apt update
sudo apt install -y python3-pip

# Install the pymysql library
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis or redis-py, assuming direct_redis is available via pip
# If it's not available via pip, this step would be different
pip3 install direct_redis
