#!/bin/bash
# Install Python 3 and pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo
