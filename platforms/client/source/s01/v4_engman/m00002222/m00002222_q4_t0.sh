#!/bin/bash

# Ensure pip is installed
sudo apt update
sudo apt install -y python3-pip

# Install pandas
pip3 install pandas

# Install the direct_redis package
pip3 install git+https://github.com/{username}/direct_redis.git
# Note: Replace {username} with the actual GitHub username where the direct_redis repository is hosted.
