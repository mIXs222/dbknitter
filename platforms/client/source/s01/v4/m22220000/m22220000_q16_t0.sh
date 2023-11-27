#!/bin/bash

# Bash script to install dependencies for the Python code.

# Update and install system dependencies
apt-get update
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pandas direct-redis
