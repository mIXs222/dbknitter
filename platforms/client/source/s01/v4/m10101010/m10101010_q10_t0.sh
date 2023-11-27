#!/bin/bash

# Update repositories and install Python
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymongo pymysql
