#!/bin/bash
# bash script to install all dependencies

# Update the package list
sudo apt-get update

# Install python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pandas direct_redis
