#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip if not already present
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo
