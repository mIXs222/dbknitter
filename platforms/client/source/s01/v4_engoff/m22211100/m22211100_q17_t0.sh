#!/bin/bash

# Update package repository
apt-get update -y

# Install Python and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install required Python libraries
pip3 install pymysql pandas direct-redis
