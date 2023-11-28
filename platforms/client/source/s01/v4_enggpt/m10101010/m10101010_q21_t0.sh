#!/bin/bash

# Update package list and install pip
sudo apt update
sudo apt install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
