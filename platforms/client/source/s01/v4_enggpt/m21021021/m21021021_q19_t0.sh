#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Python3, PIP and required libraries if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql and pymongo using PIP
pip3 install pymysql pymongo
