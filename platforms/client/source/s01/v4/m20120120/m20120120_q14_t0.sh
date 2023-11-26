#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install pymysql and pymongo libraries
pip3 install pymysql pymongo
