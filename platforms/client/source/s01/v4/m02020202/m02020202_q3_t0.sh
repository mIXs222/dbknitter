#!/bin/bash

# Update the package list
apt-get update

# Install Python3, pip and other essentials
apt-get install -y python3 python3-pip python3-dev

# Install the required Python libraries
pip3 install pandas pymysql redis direct_redis

# Note: You might want to specify the versions of the libraries if needed.
