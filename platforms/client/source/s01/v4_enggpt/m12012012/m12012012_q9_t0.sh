#!/bin/bash
# install_dependencies.sh
# Install all the dependencies required to run the Python script

# Update package list
sudo apt-get update

# Install Python3, pip and the necessary libraries
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pymongo pandas direct_redis
