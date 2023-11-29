#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update -y

# Install Python 3 and pip (if not already installed)
sudo apt-get install python3 python3-pip -y

# Install pymysql and pandas libraries
pip3 install pymysql pandas

# Install Redis and direct_redis dependencies
pip3 install redis direct_redis
