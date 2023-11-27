#!/bin/bash

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo
pip3 install pandas
pip3 install direct-redis
