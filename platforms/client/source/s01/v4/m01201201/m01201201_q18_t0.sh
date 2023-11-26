#!/bin/bash
# Python dependencies installation script

# Update the package index
sudo apt-get update

# Install Python and Pip if they aren't installed
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct-redis

# Note: Assumes direct_redis and its dependencies are
# available to pip and do not need separate system dependencies
