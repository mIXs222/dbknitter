#!/bin/bash

# Update package lists
apt-get update -y

# Install Python and pip
apt-get install -y python3.8 python3-pip

# Install required Python libraries
pip3 install pandas
pip3 install pymysql
pip3 install pymongo
pip3 install direct_redis
