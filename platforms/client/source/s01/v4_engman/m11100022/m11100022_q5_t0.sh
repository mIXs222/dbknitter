#!/bin/bash

# Update system
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install MongoDB client
sudo apt-get install -y mongodb-clients

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
