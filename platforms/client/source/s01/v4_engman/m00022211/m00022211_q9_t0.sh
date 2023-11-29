#!/bin/bash

# Update package lists
sudo apt-get update

# Install MySQL client
sudo apt-get install -y mysql-client

# Install Python3 and pip3 if not present
sudo apt-get install -y python3 python3-pip

# Install Python libraries for MySQL, MongoDB, and Redis as well as pandas
pip3 install pymysql pymongo redis pandas direct-redis
