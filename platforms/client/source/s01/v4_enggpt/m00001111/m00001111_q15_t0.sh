#!/bin/bash

# Update repositories and install Python3 and pip3 (if not already installed)
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python3 libraries for the task
pip3 install pymysql pymongo
