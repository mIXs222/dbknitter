#!/bin/bash
# Ensure to update the system and install python3 and pip
apt-get update -y
apt-get install -y python3 python3-pip

# Install pymongo for MongoDB connection
pip3 install pymongo
