# Bash Script - install_dependencies.sh
#!/bin/bash

# Ensure system packages are up-to-date
sudo apt-get update

# Install Python
sudo apt-get install python3 python3-pip -y

# Install MongoDB's Python client
pip3 install pymongo

# Install Python library for pandas
pip3 install pandas

# Install the direct_redis package
pip3 install git+https://github.com/Yannael/direct_redis#egg=direct_redis
