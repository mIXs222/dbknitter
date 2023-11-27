#!/bin/bash

# Update the apt package index and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo
