#!/bin/bash
# Install Python and pip if they aren't already installed

# Update package list
sudo apt update

# Install Python3 and pip3
sudo apt install -y python3
sudo apt install -y python3-pip

# Install the required Python packages
pip3 install pandas
pip3 install direct_redis
