#!/bin/bash

# Assuming the use of a Python virtual environment, these commands should be run within that environment

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pandas
pip3 install direct_redis
