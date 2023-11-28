#!/bin/bash
# Ensure that pip, Python development packages, and Redis are installed

# If using apt package manager (Debian/Ubuntu)
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# If using yum package manager (RHEL/CentOS)
#sudo yum update
#sudo yum install -y python3-pip python3-devel

# If using dnf package manager (Fedora)
#sudo dnf install -y python3-pip python3-devel

# Install the necessary Python packages
pip3 install pymongo pandas redis direct_redis
