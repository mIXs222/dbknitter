#!/bin/bash

# Update package list and install Python pip (if not already installed)
sudo apt update
sudo apt install -y python3-pip

# Install Python dependencies using pip
pip3 install pymysql pandas direct-redis
