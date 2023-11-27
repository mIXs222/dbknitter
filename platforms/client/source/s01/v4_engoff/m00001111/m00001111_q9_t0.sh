#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL client (PyMySQL)
pip3 install pymysql

# Install Python MongoDB client (PyMongo)
pip3 install pymongo
