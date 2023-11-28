#!/bin/bash

# Update the package repository
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install pymongo
