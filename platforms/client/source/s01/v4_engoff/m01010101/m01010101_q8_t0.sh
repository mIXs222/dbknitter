#!/bin/bash
set -e

# Update package list
sudo apt-get update

# Install Python3, pip and required packages
sudo apt-get install -y python3 python3-pip

# Install Python libraries using pip
pip3 install pymysql pymongo
