#!/bin/bash

# Update the package list
apt-get update

# Install pip if not already available
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pandas pymongo direct-redis
