#!/bin/bash

# Update and  install system requirements
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# Install Python dependencies
pip3 install pymysql pandas direct-redis
