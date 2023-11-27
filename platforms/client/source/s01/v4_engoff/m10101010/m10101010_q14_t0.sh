#!/bin/bash

# install_dependencies.sh

# Update repositories and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
