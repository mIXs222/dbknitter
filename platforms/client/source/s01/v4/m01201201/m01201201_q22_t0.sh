#!/bin/bash

# Update the package list
apt-get update

# Install Python 3 and pip, if not already installed
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct_redis
