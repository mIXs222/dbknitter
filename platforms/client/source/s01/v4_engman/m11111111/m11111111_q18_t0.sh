#!/bin/bash

# Update package list and install python3 and pip
sudo apt update
sudo apt install -y python3 python3-pip

# Install pymongo package using pip
pip3 install pymongo
