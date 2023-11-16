#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver for Python
pip3 install pymongo pandas
