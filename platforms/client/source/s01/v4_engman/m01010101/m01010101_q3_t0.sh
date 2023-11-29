#!/bin/bash

# Update package list and upgrade system
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip (if not already installed)
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the necessary Python libraries
pip3 install pymysql pymongo
