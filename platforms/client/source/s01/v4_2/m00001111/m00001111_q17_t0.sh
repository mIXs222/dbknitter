#!/bin/bash

# Update package lists
apt-get update -y

# Install Python
apt-get install -y python3.8

# Install pip
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
