#!/bin/bash
set -e

# Update the package list and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pandas direct_redis
