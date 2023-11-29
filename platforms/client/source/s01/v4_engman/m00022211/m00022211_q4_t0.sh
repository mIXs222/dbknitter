#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3, pip, and the necessary libraries for MongoDB
sudo apt-get install -y python3 python3-pip
pip3 install pymongo
