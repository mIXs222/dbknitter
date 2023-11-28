#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip and Python development files if not already installed
sudo apt-get install -y python3-pip python3-dev

# Install the required python libraries
pip3 install pymysql pymongo
