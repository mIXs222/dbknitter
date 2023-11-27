#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install python3 python3-pip -y

# Install necessary Python libraries
pip3 install pymysql pymongo pandas direct_redis
