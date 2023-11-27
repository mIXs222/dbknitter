#!/bin/bash

# install_dependencies.sh

# Update the package list
apt-get update

# Install Python3 pip if not already installed
apt-get install -y python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
