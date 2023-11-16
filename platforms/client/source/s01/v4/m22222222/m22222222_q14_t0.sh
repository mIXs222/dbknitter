#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pandas
pip3 install direct_redis
