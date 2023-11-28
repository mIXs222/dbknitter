#!/bin/bash
# Update packages and install dependencies
apt-get update

# Install python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymysql direct-redis
