#!/bin/bash

# Install Python3 and pip (if not available)
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pandas direct-redis
