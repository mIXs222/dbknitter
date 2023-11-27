#!/bin/bash

# Update packages list
apt-get update

# Install Python and pip if not already installed
apt-get install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
