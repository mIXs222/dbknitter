#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install pymongo and pymysql using pip
pip3 install pymongo pymysql
