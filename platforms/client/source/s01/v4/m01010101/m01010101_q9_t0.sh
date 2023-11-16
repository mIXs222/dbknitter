#!/bin/bash

# Update package lists
apt-get update

# Install Python3, pip and MongoDB client
apt-get install -y python3 python3-pip mongodb-clients

# Install Python library dependencies
pip3 install pymysql pymongo pandas
