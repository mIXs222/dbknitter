#!/bin/bash

# Bash script to install dependencies for the Python code

# Update package list and install pip
sudo apt-get update
sudo apt-get install python3-pip -y

# Install python packages
pip3 install pymongo
pip3 install pandas

# Since direct_redis is not a standard package available on PyPI, you need to clone and install it manually
# git clone https://github.com/nosqlclient/direct_redis.git
# cd direct_redis
# python3 setup.py install
# cd ..
# rm -rf direct_redis

# Now you can run the python code using
# python3 your_script.py
