#!/bin/bash

# Update and install pip
apt-get update
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
