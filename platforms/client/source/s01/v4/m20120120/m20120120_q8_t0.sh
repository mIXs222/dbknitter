#!/bin/bash

# Update the package list and install Python.
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python packages.
pip3 install pymysql pymongo pandas redis direct-redis
