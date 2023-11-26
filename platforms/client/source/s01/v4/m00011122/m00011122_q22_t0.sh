#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and Pip
sudo apt-get install python3 python3-pip -y

# Install required Python packages
pip3 install pymongo pandas
