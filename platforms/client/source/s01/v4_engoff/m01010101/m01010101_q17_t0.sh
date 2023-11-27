#!/bin/bash

# Updating package lists
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
