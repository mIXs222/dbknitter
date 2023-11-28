#!/bin/bash

# Update and Install Python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the PyMySQL and pymongo libraries
pip3 install pymysql pymongo
