#!/bin/bash
# Update package list
apt-get update

# Install python3 and pip if not installed
apt-get install python3 -y
apt-get install python3-pip -y

# Install required Python packages
pip3 install pymysql pymongo
