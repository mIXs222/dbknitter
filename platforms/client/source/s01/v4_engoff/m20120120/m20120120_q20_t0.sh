#!/bin/bash

# This script assumes you have Python 3 already installed.

# Update package list and upgrade existing packages.
sudo apt-get update -y
sudo apt-get upgrade -y

# Install pip, a Python package installer,
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pandas pymysql pymongo direct-redis
