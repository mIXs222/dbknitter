#!/bin/bash

# Update package list and upgrade existing packages
sudo apt update && sudo apt upgrade -y

# Install Python3 and Pip if not already installed
sudo apt install -y python3 python3-pip

# Install pymysql and pymongo Python packages
pip3 install pymysql pymongo
