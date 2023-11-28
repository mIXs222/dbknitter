#!/bin/bash

# Update repositories and Install pip if not already installed
sudo apt update
sudo apt install -y python3-pip

# Install required Python libraries
pip3 install pymongo
