#!/bin/bash

# Make sure Python 3 and pip are installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo direct_redis pandas
