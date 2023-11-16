# Bash script (install_dependencies.sh)
#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install pandas library
pip3 install pandas

# Assuming direct_redis is a package available on PyPI
pip3 install direct_redis
