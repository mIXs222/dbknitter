#!/bin/bash

# Update and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pymongo
