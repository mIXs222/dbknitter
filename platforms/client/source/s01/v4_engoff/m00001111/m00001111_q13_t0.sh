#!/bin/bash

# Update repositories and install pip if it is not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo to interact with MongoDB
pip3 install pymongo
