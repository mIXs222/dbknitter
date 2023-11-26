#!/bin/bash

# Bash script to install python3, pip, pymysql, pymongo

# Update repo and install python3 and pip
sudo apt update
sudo apt install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
