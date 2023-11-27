#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3 and Python3-pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
