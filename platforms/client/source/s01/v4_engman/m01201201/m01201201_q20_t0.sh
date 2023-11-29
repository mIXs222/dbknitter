#!/bin/bash

# Update the package list
sudo apt-get update

# Install the necessary dependencies
sudo apt-get install -y python3-pip
sudo apt-get install -y python3-dev
sudo apt-get install -y libmysqlclient-dev

# Install Python packages
pip3 install pymongo
pip3 install pymysql
pip3 install pandas
pip3 install direct_redis
