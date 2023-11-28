#!/bin/bash

# Install system dependencies
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas pymysql direct-redis
