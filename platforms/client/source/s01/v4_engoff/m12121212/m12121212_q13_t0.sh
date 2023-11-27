#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and Pip
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymongo pandas direct-redis
