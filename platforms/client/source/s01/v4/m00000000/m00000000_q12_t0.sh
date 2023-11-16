#!/bin/bash

# Update system package index
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install MySQL client with pymysql
pip3 install mysqlclient
