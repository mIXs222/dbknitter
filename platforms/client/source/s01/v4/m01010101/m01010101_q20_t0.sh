#!/bin/bash

# Ensure system packages are up-to-date
sudo apt-get update -y

# Install Python and pip if they are not installed
sudo apt-get install python3-pip -y

# Install pymysql for MySQL connections
pip3 install pymysql

# Install pymongo for MongoDB connections
pip3 install pymongo
