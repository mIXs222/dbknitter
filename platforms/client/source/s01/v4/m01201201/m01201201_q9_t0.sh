#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pymongo sqlalchemy pandas direct-redis
