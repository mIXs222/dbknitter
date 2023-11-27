#!/bin/bash

# Update package list and upgrade packages
apt-get update
apt-get -y upgrade

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct_redis
