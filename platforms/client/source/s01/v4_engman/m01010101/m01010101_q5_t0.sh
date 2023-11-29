#!/bin/bash

# Update package list
apt-get update

# Upgrade existing packages
apt-get upgrade -y

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python MySQL and MongoDB libraries
pip3 install pymysql pymongo
