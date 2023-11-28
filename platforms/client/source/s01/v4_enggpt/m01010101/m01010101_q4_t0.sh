#!/bin/bash

# Update package list
apt-get update

# Install pip for Python package management if not already installed
apt-get --yes install python3-pip

# Install Python MySQL client library pymysql
pip3 install pymysql

# Install Python MongoDB client library pymongo
pip3 install pymongo
