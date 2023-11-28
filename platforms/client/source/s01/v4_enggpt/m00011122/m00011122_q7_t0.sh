#!/bin/bash

# Update the package list
apt-get update
apt-get -y upgrade

# Install Python3 and pip3 if not already installed
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct-redis
