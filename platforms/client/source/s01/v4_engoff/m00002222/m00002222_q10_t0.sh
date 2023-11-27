#!/bin/bash

# Update package list and install pip if not already installed
apt-get update
apt-get install -y python3-pip

# Install Python dependencies for MySQL and Redis
pip3 install pymysql pandas direct-redis
