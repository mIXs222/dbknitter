#!/bin/bash

# Update and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymongo for MongoDB interaction
pip3 install pymongo
