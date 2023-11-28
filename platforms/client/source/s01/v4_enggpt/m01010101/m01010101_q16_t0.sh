#!/bin/bash

# Update repository data and install Python 3 and pip if they are not installed
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip

# Install Python libraries for MySQL and MongoDB
pip3 install pymysql pymongo
