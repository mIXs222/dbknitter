#!/bin/bash
# install_dependencies.sh

# Update the package index
apt-get update

# Install pip for Python3
apt-get install python3-pip -y

# Install the python libraries
pip3 install pymysql pandas

# Install the direct_redis library (as it is not available on PyPI, this is a placeholder)
# You might have to install it from a repository or some other source if it's custom
pip3 install git+https://github.com/user/direct_redis.git
