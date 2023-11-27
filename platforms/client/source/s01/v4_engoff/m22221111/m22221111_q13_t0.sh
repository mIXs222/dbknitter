#!/bin/bash

# Update package list and upgrade the packages
sudo apt update && sudo apt upgrade -y

# Install pip for Python3
sudo apt install python3-pip -y

# Install pymongo using pip
pip3 install pymongo
