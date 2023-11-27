#!/bin/bash

# Install Python and pip if not already installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis direct_redis
