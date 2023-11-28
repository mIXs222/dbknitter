#!/bin/bash
# install_dependencies.sh

# Updating repositories and installing Python3 and pip3
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo
