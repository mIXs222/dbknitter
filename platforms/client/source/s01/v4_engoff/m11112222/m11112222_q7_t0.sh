#!/bin/bash

# Update and Install Python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3.8 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
