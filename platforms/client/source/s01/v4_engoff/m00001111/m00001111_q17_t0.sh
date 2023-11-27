#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python3 and PIP
sudo apt-get install -y python3 python3-pip

# Install needed Python3 libraries
pip3 install pymysql pymongo pandas
