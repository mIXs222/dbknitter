#!/bin/bash

# install_dependencies.sh

# Update package list and install python3-pip
apt-get update
apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
