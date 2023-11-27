#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python and Pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
