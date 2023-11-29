#!/bin/bash

# Install Python and pip if they are not installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo pandas redis direct_redis
