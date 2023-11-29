#!/bin/bash

# This script is for installing the necessary dependencies to run the provided Python code.

# Update package list
sudo apt-get update

# Install Python 3 and pip if not present
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct_redis
