#!/bin/bash

# Update repositories
sudo apt-get update

# Install Python3 and pip3 if not already installed
sudo apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymongo direct_redis pandas
