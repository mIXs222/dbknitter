#!/bin/bash

# Update package lists
apt-get update

# Install python3 and python3-pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct-redis
