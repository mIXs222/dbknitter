#!/bin/bash
# Install Python and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install needed Python packages
pip3 install pymysql pandas direct-redis
