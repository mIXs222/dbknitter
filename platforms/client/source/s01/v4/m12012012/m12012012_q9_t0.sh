#!/bin/bash

# Update the package index
sudo apt update

# Install Python 3 and pip if not already installed
sudo apt install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
