#!/bin/bash

set -e

# Upgrade pip
python -m pip install --upgrade pip

# Install pandas, redis 
pip install pandas redis
