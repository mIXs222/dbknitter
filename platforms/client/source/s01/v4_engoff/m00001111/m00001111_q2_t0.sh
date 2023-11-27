#!/bin/bash
# Install Python and Pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install dependencies using pip
pip3 install pymysql pymongo
