#!/bin/bash
# install_dependencies.sh

# Update repository and Install Python3 pip
apt-get update
apt-get install python3-pip -y

# Install pymysql and pymongo
pip3 install pymysql pymongo
