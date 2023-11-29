#!/bin/bash

# Update package list and install Python 3 and pip
sudo apt update
sudo apt install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
