#!/bin/bash
# Install Python and necessary libraries

# Update package list
apt-get update

# Install Python
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo direct-redis pandas
