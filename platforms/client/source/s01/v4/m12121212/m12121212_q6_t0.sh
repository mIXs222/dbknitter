#!/bin/bash

# Update system and get pip if not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python pandas package
pip3 install pandas

# Install direct_redis, which mimics the redis-py interface for your specific use case
pip3 install direct_redis

# Install additional dependencies if needed
