#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and Pip if they aren't already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas
pip3 install direct_redis
