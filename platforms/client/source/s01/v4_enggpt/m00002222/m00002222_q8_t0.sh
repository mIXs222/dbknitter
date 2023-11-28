#!/bin/bash
# This script is intended to set up the environment needed to run the python code above.

# Updates repository and gets python3 and pip if they're not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Installs the necessary Python packages globally.
sudo pip3 install pandas pymysql direct_redis
