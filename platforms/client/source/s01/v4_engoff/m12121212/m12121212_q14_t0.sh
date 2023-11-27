#!/bin/bash

# Update system package manager
sudo apt update

# Python3 and pip installation
sudo apt install python3 python3-pip -y

# Install required Python packages
pip3 install pymongo==3.12.3 pandas==1.3.4 direct_redis

# Note: direct_redis is a hypothetical package since there's no package with this exact name.
# If direct_redis refers to a custom or non-standard library, the installation step would need to be amended accordingly.
