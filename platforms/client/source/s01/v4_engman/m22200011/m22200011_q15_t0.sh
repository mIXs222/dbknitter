#!/bin/bash

# Update repositories and package list
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MySQL and MongoDB drivers for Python
pip3 install pymysql pymongo
