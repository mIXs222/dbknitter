#!/bin/bash

# Update package repository
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MongoDB and MySQL Python drivers
pip3 install pymysql pymongo
