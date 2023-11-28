#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 pip if not already present
sudo apt-get install -y python3-pip

# Install the Python libraries
pip3 install pymysql pymongo pandas
