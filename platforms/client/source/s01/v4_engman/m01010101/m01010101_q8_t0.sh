#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pymongo pandas
