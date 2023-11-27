#!/bin/bash

# Update the system and install Python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pandas pymongo direct-redis
