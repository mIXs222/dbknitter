#!/bin/bash

# Update package lists
apt-get update

# Upgrade existing packages
apt-get upgrade -y

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
