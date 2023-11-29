#!/bin/bash
# Bash script to install all dependencies

# Update the package index
sudo apt-get update

# Install python3 and python3-pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MySQL server (if needed for local testing)
sudo apt-get install -y mysql-server

# Install the required Python libraries
pip3 install pymysql pandas direct-redis
