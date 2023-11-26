#!/bin/bash

# bash script to install dependencies for the python code

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install pymongo redis pandas
