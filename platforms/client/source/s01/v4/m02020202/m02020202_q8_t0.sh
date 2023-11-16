#!/bin/bash

# Update the package manager and install pip and Redis if they aren't already installed
sudo apt-get update
sudo apt-get install -y python3-pip
sudo apt-get install -y redis-server

# Install the Python packages
pip3 install pymysql pandas direct-redis
