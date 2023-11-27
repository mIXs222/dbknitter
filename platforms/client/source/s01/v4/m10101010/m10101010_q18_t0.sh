#!/bin/bash

# Update the list of available packages and their versions
sudo apt-get update

# Install python3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install PyMySQL and PyMongo using pip
pip3 install pymysql pymongo
