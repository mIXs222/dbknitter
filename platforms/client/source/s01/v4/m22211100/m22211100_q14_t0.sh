#!/bin/bash
# Set up a Python virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install Python dependencies
pip install pymysql pandas direct-redis
