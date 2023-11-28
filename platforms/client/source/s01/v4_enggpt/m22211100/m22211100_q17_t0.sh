#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pandas

# Installation of direct_redis: since this may be a custom package that's not available publicly on PyPi,
# the following installation method is an example and the actual installation
# might differ based on how the direct_redis package is distributed.
pip3 install direct_redis
