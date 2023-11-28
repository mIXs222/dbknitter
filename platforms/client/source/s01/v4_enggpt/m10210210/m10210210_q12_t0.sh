#!/bin/bash

# Update repository and Install Python3 and Pip3
apt-get update
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo
