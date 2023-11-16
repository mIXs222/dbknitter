#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo, necessary to interact with MongoDB
python3 -m pip install pymongo
