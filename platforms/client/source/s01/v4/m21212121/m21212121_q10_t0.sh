#!/bin/bash
# install_dependencies.sh

# Update package list and upgrade existing packages
apt-get update && apt-get upgrade -y

# Install Python and pip and Redis essentials
apt-get install -y python3 python3-pip gcc

# Install Python libraries
pip3 install pymongo pandas redis direct-redis

# Note: The user must ensure that MongoDB and Redis servers are running and accessible at the specified hostnames and ports.
