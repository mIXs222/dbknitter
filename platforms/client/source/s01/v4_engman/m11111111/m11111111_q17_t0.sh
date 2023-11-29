#!/bin/bash

# Update package list and install python3-pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo and pandas using pip
pip3 install pymongo pandas
