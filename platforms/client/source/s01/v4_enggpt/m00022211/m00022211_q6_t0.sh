#!/bin/bash

# Update the package list
apt-get update

# Install python3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install pymongo to be able to connect to MongoDB
pip3 install pymongo
