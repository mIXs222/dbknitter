#!/bin/bash

# Update package list
sudo apt update -y

# Install python3, pip3
sudo apt install python3-pip -y

# Upgrade pip
pip install --upgrade pip

# Install pymongo
pip install pymongo
