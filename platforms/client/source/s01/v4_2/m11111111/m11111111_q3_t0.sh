#!/bin/bash
# Update system
sudo apt-get update

# Upgrade pip
python -m pip install --upgrade pip

# Install necessary python libraries
pip install pandas
pip install pymongo
pip install python-dateutil
