#!/bin/bash

# Update package list
sudo apt-get update

# Install pip3 if not installed
sudo apt-get install -y python3-pip

# Install required packages for the Python script
pip3 install pymongo
pip3 install pymysql
