#!/bin/bash

# Update package list
sudo apt update

# Install Python and pip if they are not already installed
sudo apt install -y python3 python3-pip

# Install Python dependencies
pip3 install pandas redis
