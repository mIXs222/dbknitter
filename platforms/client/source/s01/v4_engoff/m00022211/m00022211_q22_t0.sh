#!/bin/bash

# Update package lists
apt-get update

# Install python3 and pip if not present
apt-get install -y python3 python3-pip

# Install required Python packages with pip
pip3 install pymongo pandas direct-redis

# Note: Depending on the environment, 'sudo' might be required for apt-get and pip3 commands.
