#!/bin/bash

# Set up the environment for the Python script

# Update repositories and install pip
apt-get update
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo
