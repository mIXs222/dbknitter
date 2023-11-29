#!/bin/bash
# This script is created to install python and required dependencies to run the python code.

# Install Python and pip if they are not already installed.
sudo apt update
sudo apt install -y python3 python3-pip

# Install the required Python packages.
pip3 install pandas redis direct-redis
