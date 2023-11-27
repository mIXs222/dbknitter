#!/bin/bash
# Update package list and upgrade all packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql direct_redis
