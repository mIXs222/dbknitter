#!/bin/bash

# Update packages and install Python and pip
apt-get update
apt-get install -y python3 python3-pip

# Assuming setuptools and wheel are already installed, if not - uncomment the next line
# python3 -m pip install setuptools wheel

# Install required Python libraries
python3 -m pip install pandas pymysql redis direct-redis
