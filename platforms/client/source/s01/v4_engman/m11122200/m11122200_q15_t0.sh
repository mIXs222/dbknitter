#!/bin/bash
# Updating package lists
sudo apt-get update

# Installing Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Installing required Python packages
pip3 install pymysql pandas direct-redis
