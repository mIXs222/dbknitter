#!/bin/bash

# Update the package lists
sudo apt-get update

# Install pip
sudo apt-get install -y python3-pip

# Install pymongo (for MongoDB connector)
pip3 install pymongo

# Install pandas (for data manipulation)
pip3 install pandas

# Install sqlalchemy and pymysql (for MySQL connector)
pip3 install sqlalchemy pymysql
