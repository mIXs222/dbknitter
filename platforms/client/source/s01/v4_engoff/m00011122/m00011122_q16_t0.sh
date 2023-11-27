#!/bin/bash

# Update package list
sudo apt-get update

# Install python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
