#!/bin/bash

# Update package lists
apt-get update

# Install MySQL client
apt-get install -y default-mysql-client

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install Python library dependencies
pip3 install pymysql==1.0.2 pymongo==4.1.1 pandas==1.4.1
