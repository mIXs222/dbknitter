#!/bin/bash
# Script to install dependencies for running Python script

# Update apt package index
sudo apt update

# Install pip if not installed
sudo apt install -y python3-pip

# Install required Python packages
pip3 install pandas
pip3 install direct_redis
