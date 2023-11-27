#!/bin/bash

# Update package list and install Python 3 pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas
