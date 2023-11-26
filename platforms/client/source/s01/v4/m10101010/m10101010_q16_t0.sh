#!/bin/bash

# Install Python and PIP
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Upgrade PIP itself
pip3 install --upgrade pip

# Install Python library dependencies
pip3 install pymysql pymongo
