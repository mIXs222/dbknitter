#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python pip if not already installed
sudo apt-get install -y python3-pip

# Install MySQL client dev libraries (necessary for some Python MySQL libraries)
sudo apt-get install -y default-libmysqlclient-dev

# Install dependencies using pip
pip3 install pymysql pandas direct_redis
