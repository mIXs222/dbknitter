#!/bin/bash

# Update the package list
apt-get update

# Install pip if it's not installed
apt-get install -y python3-pip

# Install the pymongo package using pip
pip3 install pymongo
