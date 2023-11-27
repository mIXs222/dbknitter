#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
