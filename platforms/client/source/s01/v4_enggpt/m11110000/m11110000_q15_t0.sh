#!/bin/bash

# Update system's package index
sudo apt update

# Install pip and Python dev packages if not installed
sudo apt install python3-pip python3-dev -y

# Install PyMySQL library
pip3 install pymysql

# Install PyMongo library
pip3 install pymongo
