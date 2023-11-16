#!/bin/bash

# Update package list
apt-get update

# Install Python 3, pip and the required system libraries
apt-get install -y python3 python3-pip

# Install Pandas and direct_redis using pip
pip3 install pandas
pip3 install git+https://github.com/han-so1omon/direct_redis.git
