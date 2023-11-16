#!/bin/bash

# Update package list
apt-get update

# Install pip if not available
apt-get install -y python-pip

# Install Python dependencies
pip install pandas
pip install direct-redis
