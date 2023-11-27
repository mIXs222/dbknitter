#!/bin/bash

# Ensure system is updated and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymongo direct-redis pandas
