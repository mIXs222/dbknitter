#!/bin/bash

# Update package lists
apt-get update

# Install pip3 and Python MongoDB library
apt-get install -y python3-pip
pip3 install pymongo

# Install the PyMySQL library
pip3 install pymysql
