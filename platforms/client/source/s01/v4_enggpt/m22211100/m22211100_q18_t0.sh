#!/bin/bash
# Install Python and MongoDB clients

# Update package manager and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install PyMySQL and pymongo
pip3 install pymysql
pip3 install pymongo
