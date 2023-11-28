#!/bin/bash

# Update and upgrade the package list
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas redis direct_redis
