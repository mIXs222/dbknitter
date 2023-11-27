#!/bin/bash

# Update and install Python 3 pip
apt-get update
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pandas direct-redis
