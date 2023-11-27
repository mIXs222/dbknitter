#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and Pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas redis direct-redis
