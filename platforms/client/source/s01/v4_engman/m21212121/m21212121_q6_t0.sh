#!/bin/bash

# Update package lists
apt-get update -y

# Install Python and pip if they're not already installed
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
