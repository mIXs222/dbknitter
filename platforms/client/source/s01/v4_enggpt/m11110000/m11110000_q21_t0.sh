#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the Python packages needed for the script
sudo pip3 install pymysql
sudo pip3 install pymongo
