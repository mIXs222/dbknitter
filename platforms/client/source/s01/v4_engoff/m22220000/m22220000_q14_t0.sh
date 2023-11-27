#!/bin/bash

# Please run this script with `sudo` if necessary.
# Update package list and install pip if not already installed
apt-get update
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct-redis
