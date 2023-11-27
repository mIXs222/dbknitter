#!/bin/bash
# install_dependencies.sh

# Update package list and install Python 3 pip
apt-get update
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pymongo
pip3 install pandas

# Install the direct_redis package (assuming it is available for installation)
pip3 install direct-redis
