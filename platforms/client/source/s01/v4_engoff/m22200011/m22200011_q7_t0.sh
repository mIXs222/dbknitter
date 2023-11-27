#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip if it's not available
sudo apt-get install -y python3-pip

# Install the necessary Python libraries
pip3 install pymysql pymongo pandas direct-redis
