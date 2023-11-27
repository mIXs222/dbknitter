#!/bin/bash

# Update package list
apt-get update

# Install python3 and python3-pip if not installed
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
