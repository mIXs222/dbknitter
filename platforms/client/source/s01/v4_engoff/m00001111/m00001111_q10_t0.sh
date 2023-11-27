#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3, pip and required packages for MySQL and MongoDB
sudo apt-get install -y python3 python3-pip
pip3 install pymysql pymongo
