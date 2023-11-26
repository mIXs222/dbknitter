#!/bin/bash
# save as setup.sh

# Activate environment, replace 'your_env' with the name of your Python environment
# source activate your_env

# Update pip
pip install --upgrade pip

# Install dependencies
pip install pymongo pandas direct_redis
