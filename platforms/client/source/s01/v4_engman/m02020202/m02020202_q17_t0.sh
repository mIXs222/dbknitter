#!/bin/bash

# Install pip if not already installed
which pip || (echo "Pip not found. Installing..." && sudo apt update && sudo apt install -y python3-pip)

# Install Python dependencies
pip install pymysql pandas direct-redis
