#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python and pip
apt-get install python3 python3-pip -y

# Install pymysql, pandas, and sqlalchemy
pip3 install pymysql pandas sqlalchemy

# Install redis-py and direct_redis (assuming direct_redis is available via pip)
pip3 install redis direct_redis
