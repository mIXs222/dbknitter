#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python3 if it does not exist
which pip3 || (sudo apt-get install python3-pip -y)

# Install MySQL client (pymysql)
pip3 install pymysql

# Install MongoDB client (pymongo)
pip3 install pymongo
