#!/bin/bash

# Update package list
apt-get update

# Install pip (Python package installer) if it's not available
apt-get install -y python3-pip

# Install pymysql & pymongo using pip
pip3 install pymysql pymongo
