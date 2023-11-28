#!/bin/bash

# setup_dependencies.sh

# Update package list
apt-get update -y

# Install Python pip
apt-get install python3-pip -y

# Install pymysql and pymongo
pip3 install pymysql pymongo
