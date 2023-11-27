#!/bin/bash

# Update package list
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql direct_redis pandas
