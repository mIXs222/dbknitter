#!/bin/bash

# Update packages and install python3 and pip3
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install all necessary Python packages
pip3 install pymysql pandas redis direct_redis

# Note: direct_redis might not be a real package as of the knowledge cutoff date (2023-03-07);
# if it's not available on PyPI, you would have to install it from its source or adjust the Python code accordingly.
