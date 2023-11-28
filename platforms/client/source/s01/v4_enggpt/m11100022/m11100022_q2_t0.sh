#!/bin/bash

# Update package repositories and install pip if it's not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
