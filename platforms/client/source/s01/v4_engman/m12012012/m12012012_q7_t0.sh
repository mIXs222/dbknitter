#!/bin/bash
# install_dependencies.sh

# Make sure pip is installed
sudo apt-get install -y python3-pip

# Install MySQL connector
pip3 install pymysql

# Install MongoDB connector
pip3 install pymongo

# Install Redis connector (assuming direct_redis is a fictional library mentioned for the sake of the example)
pip3 install direct_redis

# Install Pandas for data manipulation
pip3 install pandas
