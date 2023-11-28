#!/bin/bash

# Update package list
sudo apt update

# Install Python3 and pip if they are not installed
sudo apt install -y python3 python3-pip

# Install the necessary Python package
pip3 install pymongo
