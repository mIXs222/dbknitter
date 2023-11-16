#!/bin/bash

# Update package list and install python3 and pip3
sudo apt update
sudo apt install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
