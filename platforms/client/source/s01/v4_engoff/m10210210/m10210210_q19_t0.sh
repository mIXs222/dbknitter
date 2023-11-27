#!/bin/bash

# Update system package index
sudo apt update

# Install Python 3 pip if not already installed
sudo apt -y install python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python Redis driver
pip3 install git+https://github.com/danmcinerney/direct-redis.git

# Install Pandas
pip3 install pandas
