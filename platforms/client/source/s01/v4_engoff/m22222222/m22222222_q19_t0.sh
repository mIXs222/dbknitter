#!/bin/bash

# First update the package list and install Python and pip if they're not installed already
sudo apt update
sudo apt install python3 python3-pip -y

# Install Python package dependencies
pip3 install pandas redis direct_redis
