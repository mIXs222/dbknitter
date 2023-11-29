#!/bin/bash

# Update repositories and upgrade any existing packages
sudo apt-get update && sudo apt-get upgrade -y

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install direct-redis
