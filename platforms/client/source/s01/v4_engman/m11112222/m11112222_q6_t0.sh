#!/bin/bash

# Update package lists
apt-get update

# Install pip
apt-get install python3-pip -y

# Install Python Redis client, Direct Redis, and pandas
pip3 install redis direct-redis pandas
