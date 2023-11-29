# setup.sh
#!/bin/bash
set -e

# Updating package lists
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql Python library
pip3 install pymysql
