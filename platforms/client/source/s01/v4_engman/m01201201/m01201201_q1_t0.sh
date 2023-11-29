#!/bin/bash

# Update package list
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
