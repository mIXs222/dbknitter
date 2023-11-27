#!/bin/bash

# Update package lists
apt-get update

# Install python3 and pip3
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql sqlalchemy direct-redis
