#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install python libraries
pip3 install pymysql pandas direct_redis
