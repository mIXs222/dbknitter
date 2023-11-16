#!/bin/bash

# Update package list and upgrade existing packages
apt-get update && apt-get upgrade -y

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
