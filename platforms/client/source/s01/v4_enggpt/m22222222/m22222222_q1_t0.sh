#!/bin/bash
# This script will install Python3, pip, and the necessary Python packages needed to run the analysis.py script

# Update package list
sudo apt-get update

# Install python3 and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pandas
pip3 install direct-redis
