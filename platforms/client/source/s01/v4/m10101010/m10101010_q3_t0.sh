#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install Python pip if it's not already installed
sudo apt-get install -y python3-pip

# Install PyMySQL and PyMongo using pip
pip3 install pymysql pymongo
