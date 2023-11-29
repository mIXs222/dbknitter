#!/bin/bash

# Update package list and install python pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
