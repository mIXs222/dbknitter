#!/bin/bash

# Update package list
apt-get update

# Install Python pip if it's not already installed
apt-get install -y python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
