#!/bin/bash

# Update package information (may require root permissions)
apt-get update

# Install Python 3 and pip if not installed
apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pandas direct-redis
