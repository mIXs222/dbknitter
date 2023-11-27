#!/bin/bash

# Update repositories and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the PyMySQL library
pip3 install pymysql

# Install the pymongo library
pip3 install pymongo
