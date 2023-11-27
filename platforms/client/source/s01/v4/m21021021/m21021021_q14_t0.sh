#!/bin/bash

# Ensure the system package list is up-to-date
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python MySQL and MongoDB libraries
pip3 install pymysql pymongo
