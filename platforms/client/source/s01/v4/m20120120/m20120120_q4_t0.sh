#!/bin/bash

# Updates the package lists for upgrades and new package installations
apt-get -y update

# Install Python and Pip if they are not already installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct-redis
