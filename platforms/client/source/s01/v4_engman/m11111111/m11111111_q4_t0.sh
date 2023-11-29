#!/bin/bash

# Update package manager (might require sudo)
apt update

# Install Python 3 and pip (might require sudo)
apt install -y python3 python3-pip

# Install pymongo using pip (might require sudo)
pip3 install pymongo
