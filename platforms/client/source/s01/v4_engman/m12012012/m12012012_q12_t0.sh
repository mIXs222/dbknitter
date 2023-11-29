#!/bin/bash
# install_dependencies.sh

# Update packages and install pip
apt-get update
apt-get install -y python3-pip

# Install Python packages
pip3 install pymongo pandas

# Since the direct_redis package is not standard, it would typically be hosted somewhere
# For this script, we'll assume you have direct access to the package
# The following line should be modified if the 'direct_redis.DirectRedis' is accessible through PyPI or another source
git clone <direct_redis_repository_url>
cd <direct_redis_repository_dir>
pip3 install .
