#!/bin/bash

# Update package list and Upgrade system
apt-get update -y && apt-get upgrade -y

# Install Python3 and Python3-pip if not installed
apt-get install python3 python3-pip -y

# Install the necessary python libraries
pip3 install pymysql pymongo pandas direct-redis
