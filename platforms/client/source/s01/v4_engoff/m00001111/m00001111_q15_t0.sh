#!/bin/bash
set -e

# Update the package list
apt-get update

# Install Python3 and pip if not available
apt-get install -y python3 python3-pip

# Install the PyMySQL and PyMongo
pip3 install pymysql pymongo
