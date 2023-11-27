#!/bin/bash

# First, make sure pip is installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install direct-redis which is a custom library and not available in the default pypi repository
pip3 install git+https://example.com/direct-redis.git
