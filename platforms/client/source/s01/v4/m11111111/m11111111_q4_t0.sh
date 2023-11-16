#!/bin/bash

# Update package list and install Python3 and pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymongo package using pip
pip3 install pymongo

# Install csv using pip, if necessary
pip3 install csv
