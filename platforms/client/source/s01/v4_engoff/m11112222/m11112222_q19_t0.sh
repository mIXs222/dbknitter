#!/bin/bash

# Create and activate a virtual environment (optional, but recommended)
python3 -m venv venv
source venv/bin/activate

# Install required Python packages
pip install pandas pymongo "redis>=4.0.0" direct_redis
