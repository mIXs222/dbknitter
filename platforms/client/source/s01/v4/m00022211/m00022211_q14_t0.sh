#!/bin/bash

# Update packages list and install Python pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas
