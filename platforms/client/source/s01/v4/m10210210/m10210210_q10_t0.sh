#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and pip if it's not already available
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
