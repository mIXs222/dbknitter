#!/bin/bash

# Update repositories and install Python pip
sudo apt update
sudo apt install -y python3-pip

# Install pandas and custom library for the assignment
pip3 install pandas

# Provided the `direct_redis` library is available as a package or in a known repository,
# the pip installation command would be as follows. Otherwise, you'll need to install
# it from its source, adjust the command accordingly.
pip3 install direct_redis 

# Note: Please replace "pip3" with "pip" if your system uses Python 3 by default
