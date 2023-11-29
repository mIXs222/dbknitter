#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip and other dependencies for Python
sudo apt-get install -y python3-pip

# Install the PyMySQL library
pip3 install pymysql

# Install the PyMongo library
pip3 install pymongo
