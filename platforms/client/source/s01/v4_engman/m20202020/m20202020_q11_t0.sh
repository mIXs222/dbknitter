#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas

# Note: We're assuming that the direct_redis library can be installed using pip.
# If this is not possible, you may need to install it from source or another method.
# Since `direct_redis` isn't a widely recognized library as of my knowledge cutoff in 2023,
# I'm using "fake-direct-redis" in this example, which can be replaced with actual library source.
pip3 install fake-direct-redis
