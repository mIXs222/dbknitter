#!/bin/bash

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo
