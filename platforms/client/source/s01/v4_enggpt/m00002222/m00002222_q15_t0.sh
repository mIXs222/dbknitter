#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql direct_redis
