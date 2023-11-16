#!/bin/bash

# Install Python package manager
apt-get update && apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas redis pandasql
