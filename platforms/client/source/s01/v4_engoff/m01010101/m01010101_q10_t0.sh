#!/bin/bash

# Update package list
apt-get update

# Install MySQL client
apt-get install -y default-mysql-client

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo pymysql
