#!/bin/bash

# Update package list
apt-get update

# Install MySQL client
apt-get install -y default-mysql-client

# Install Python and pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
