#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python3 if not present
sudo apt-get install -y python3-pip

# Install pandas library
pip3 install pandas

# Install direct_redis library
pip3 install git+https://github.com/amyangfei/direct-redis.git#subdirectory=python
