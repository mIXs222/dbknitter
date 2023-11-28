#!/bin/bash

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
