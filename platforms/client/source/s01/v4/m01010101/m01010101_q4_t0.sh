#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
