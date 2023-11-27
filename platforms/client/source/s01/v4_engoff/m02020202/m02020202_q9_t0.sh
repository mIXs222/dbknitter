#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct-redis
