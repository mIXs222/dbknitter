#!/bin/bash

# Update the package list and install Python3 and pip if not installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install the PyMongo package
pip3 install pymongo
