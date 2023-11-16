#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip package manager
apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pymysql pandas direct-redis
