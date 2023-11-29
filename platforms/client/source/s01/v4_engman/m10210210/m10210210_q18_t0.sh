#!/bin/bash

# Update the package list
sudo apt-get update

# Install python3 and pip3 if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
