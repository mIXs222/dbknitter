#!/bin/bash

# Update and install system-wide dependencies
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Use pip to install the required Python packages
pip3 install pymongo direct_redis pandas
