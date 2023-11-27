#!/bin/bash

# Update and install Python and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas redis
