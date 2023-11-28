#!/bin/bash

# Update package lists
apt-get update

# Install Python and PIP
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo
