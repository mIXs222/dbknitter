#!/bin/bash

# Update package lists
apt-get update

# Install Python pip if not already installed
apt-get install -y python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
