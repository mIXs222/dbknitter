#!/bin/bash

# install_dependencies.sh
# Install Python and the required Python packages

apt-get update
apt-get install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
