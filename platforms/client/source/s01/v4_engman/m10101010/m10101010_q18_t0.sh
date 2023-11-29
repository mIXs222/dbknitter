#!/bin/bash
# dependencies.sh

# Update system
sudo apt update

# Install python3 and pip if not already installed
sudo apt install -y python3 python3-pip

# Install PyMySQL and PyMongo
pip3 install pymysql pymongo
