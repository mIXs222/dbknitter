#!/bin/bash
# install_dependencies.sh

# Update and Install Python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo
