#!/bin/bash

# Update the package list
sudo apt update

# Install Python3 and pip if they are not installed
sudo apt install python3 python3-pip -y

# Install the required Python modules
pip3 install pandas
pip3 install redis
pip3 install direct_redis
