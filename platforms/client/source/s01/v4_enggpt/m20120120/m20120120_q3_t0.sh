#!/bin/bash

# Update package list and install pip if it's not already installed
apt-get update
apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
