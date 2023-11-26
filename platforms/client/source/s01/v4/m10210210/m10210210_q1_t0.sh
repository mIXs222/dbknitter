#!/bin/bash

# Update package list and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install required Python library for MySQL
sudo pip3 install pymysql
