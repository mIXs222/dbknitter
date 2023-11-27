#!/bin/bash

# Update the system's package index
sudo apt-get update

# Install Python and pip if they're not already installed
sudo apt-get install python3
sudo apt-get install python3-pip

# Install the 'pymysql' Python package
sudo pip3 install pymysql

# Install the 'direct_redis' Python package
sudo pip3 install git+https://github.com/RedisLabsModules/direct_redis.git

# Install the 'pandas' Python package
sudo pip3 install pandas
