#!/bin/bash

# Update package manager and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql for MySQL connection
pip3 install pymysql

# Install pymongo for MongoDB connection
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas
