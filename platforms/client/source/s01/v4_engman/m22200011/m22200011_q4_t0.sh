#!/bin/bash

# Update package list
sudo apt update

# Install Python 3 and pip if not already installed
sudo apt install -y python3 python3-pip

# Install pymongo for MongoDB connection
pip3 install pymongo
