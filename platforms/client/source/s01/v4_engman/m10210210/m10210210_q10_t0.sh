#!/bin/bash

# Update the package list
apt-get update

# Install Python and Pip if not already installed
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pymongo pandas direct_redis
