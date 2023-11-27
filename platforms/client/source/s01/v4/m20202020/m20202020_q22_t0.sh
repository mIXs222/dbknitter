#!/bin/bash

# Update package lists
sudo apt-get update -y

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pandas direct-redis
