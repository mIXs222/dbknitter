#!/bin/bash

# Update the package list
apt-get update

# Install python3-pip, python package manager
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
