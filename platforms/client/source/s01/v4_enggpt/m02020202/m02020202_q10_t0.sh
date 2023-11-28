#!/bin/bash

# Update repositories and install Python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas sqlalchemy direct-redis
