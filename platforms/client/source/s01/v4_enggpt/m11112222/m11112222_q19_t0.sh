#!/bin/bash

# Update and install system dependencies
sudo apt update
sudo apt install -y python3-pip

# Install Python dependencies
pip3 install pymongo direct-redis pandas
