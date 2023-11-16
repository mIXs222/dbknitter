#!/bin/bash

# Update packages and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Upgrading pip to latest version
pip3 install --upgrade pip

# Install direct_redis and pandas
pip3 install direct_redis pandas
