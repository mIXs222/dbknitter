#!/bin/bash

# Update package lists
sudo apt update

# Install pip if it's not already installed
sudo apt install -y python3-pip

# Install the PyMySQL library
pip3 install pymysql

# Install the PyMongo library
pip3 install pymongo
