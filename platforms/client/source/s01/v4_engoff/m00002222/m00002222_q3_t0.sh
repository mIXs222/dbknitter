#!/bin/bash
# Install Python 3 (not required if you already have Python 3)
sudo apt-get update
sudo apt-get install -y python3

# Install pip (Python package installer)
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install redis client
pip3 install direct_redis
