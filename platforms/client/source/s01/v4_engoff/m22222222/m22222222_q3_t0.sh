#!/bin/bash

# Update package list and install pip if it's not already installed
sudo apt update
sudo apt install -y python3-pip

# Install required Python packages
pip3 install pandas direct_redis
