#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install the required python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
