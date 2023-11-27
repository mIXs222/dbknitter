#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip if not installed
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pandas
pip3 install direct_redis
