#!/bin/bash
# Filename: install_dependencies.sh

# Update package list and install pip for Python 3
sudo apt update
sudo apt install python3-pip -y

# Install pandas
pip3 install pandas

# Assuming the direct_redis module exists and is available through pip
pip3 install direct_redis

# Alternatively, if direct_redis is not available via pip, you may need to install directly
# from a source distribution or a wheel file.
# pip3 install /path/to/direct_redis_package.whl
