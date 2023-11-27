#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install python3 python3-pip -y

# Install pymongo
pip3 install pymongo
