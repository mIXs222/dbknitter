#!/bin/bash
# This bash script is designed to install the necessary dependencies to run the provided Python code.

# Update package list
apt-get update

# Install pip if not installed (might require sudo)
apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas
pip3 install direct-redis
