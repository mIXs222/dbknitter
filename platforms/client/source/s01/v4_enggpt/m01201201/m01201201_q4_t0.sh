#!/bin/bash

# Update the package list
apt-get update

# Install Python3 and pip if they are not available
apt-get install -y python3
apt-get install -y python3-pip

# Install the pymysql and pymongo libraries using pip
pip3 install pymysql pymongo
