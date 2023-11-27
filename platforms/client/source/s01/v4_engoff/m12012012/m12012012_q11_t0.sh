#!/bin/bash

# Update package list and install Python and pip if not already installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pandas pymongo 'direct_redis==1.0.0'

# NOTE: It's assuming `direct_redis` version 1.0.0 is correct, adjust as per actual library requirements
